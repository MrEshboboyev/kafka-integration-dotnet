using Confluent.Kafka;
using KafkaIntegration.Api.Models;
using KafkaIntegration.Api.Options;
using Microsoft.Extensions.Options;
using System.Text;
using System.Text.Json;

namespace KafkaIntegration.Api.Services.Implementations;

public class KafkaConsumerService : IKafkaConsumerService, IDisposable
{
    private IConsumer<string, string> _consumer;
    private readonly ILogger<KafkaConsumerService> _logger;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly KafkaOptions _options;
    private volatile bool _consuming = false;
    private readonly SemaphoreSlim _semaphore = new(1, 1);
    private bool _disposed = false;
    private readonly JsonSerializerOptions _jsonSerializerOptions;

    public KafkaConsumerService(
        IOptions<KafkaOptions> kafkaOptions, 
        ILogger<KafkaConsumerService> logger,
        IServiceScopeFactory scopeFactory)
    {
        _logger = logger;
        _scopeFactory = scopeFactory;
        _options = kafkaOptions.Value;
        _jsonSerializerOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            PropertyNameCaseInsensitive = true
        };

        _consumer = CreateConsumer();
    }

    private IConsumer<string, string> CreateConsumer()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            GroupId = _options.ConsumerGroupId,
            AutoOffsetReset = _options.AutoOffsetReset == 1 
                ? AutoOffsetReset.Earliest 
                : AutoOffsetReset.Latest,
            EnableAutoCommit = _options.EnableAutoCommit,
            AutoCommitIntervalMs = _options.AutoCommitIntervalMs,
            SessionTimeoutMs = _options.SessionTimeoutMs,
            MaxPollIntervalMs = _options.MaxPollIntervalMs,
            MaxPartitionFetchBytes = _options.MaxPartitionFetchBytes,
            FetchMinBytes = _options.FetchMinBytes,
            FetchWaitMaxMs = _options.FetchWaitMaxMs,
            SocketTimeoutMs = _options.SocketTimeoutMs,
            SocketConnectionSetupTimeoutMs = _options.SocketConnectionSetupTimeoutMs,
            ReconnectBackoffMs = _options.ReconnectBackoffMs,
            ReconnectBackoffMaxMs = _options.ReconnectBackoffMaxMs,
            EnablePartitionEof = true,
            SecurityProtocol = string.IsNullOrEmpty(_options.SecurityProtocol) 
                ? SecurityProtocol.Plaintext 
                : Enum.Parse<SecurityProtocol>(_options.SecurityProtocol, ignoreCase: true),
            SaslUsername = _options.SaslUsername,
            SaslPassword = _options.SaslPassword,
            SaslMechanism = string.IsNullOrEmpty(_options.SaslMechanism) 
                ? SaslMechanism.Plain 
                : Enum.Parse<SaslMechanism>(_options.SaslMechanism, ignoreCase: true)
        };

        // Add SSL configuration if provided
        if (!string.IsNullOrEmpty(_options.SslCaLocation))
        {
            config.SslCaLocation = _options.SslCaLocation;
        }
        
        if (!string.IsNullOrEmpty(_options.SslCertificateLocation))
        {
            config.SslCertificateLocation = _options.SslCertificateLocation;
        }
        
        if (!string.IsNullOrEmpty(_options.SslKeyLocation))
        {
            config.SslKeyLocation = _options.SslKeyLocation;
        }
        
        if (!string.IsNullOrEmpty(_options.SslKeyPassword))
        {
            config.SslKeyPassword = _options.SslKeyPassword;
        }

        return new ConsumerBuilder<string, string>(config)
            .SetErrorHandler(OnConsumerError)
            .SetLogHandler(OnConsumerLog)
            .SetPartitionsAssignedHandler(OnPartitionsAssigned)
            .SetPartitionsRevokedHandler(OnPartitionsRevoked)
            .Build();
    }

    public async Task StartConsumingAsync(string topic, CancellationToken cancellationToken)
    {
        await StartConsumingAsync([topic], cancellationToken);
    }

    public async Task StartConsumingAsync(IEnumerable<string> topics, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await _semaphore.WaitAsync(cancellationToken);
        try
        {
            if (_consuming)
            {
                _logger.LogWarning("Consumer is already running");
                return;
            }

            _consumer.Subscribe(topics);
            _consuming = true;
            _logger.LogInformation("Started consuming from topics: {Topics}", string.Join(", ", topics));

            while (_consuming && !cancellationToken.IsCancellationRequested)
            {
                try
                {
                    // We ensure faster response to cancellationToken by adding a timeout
                    var consumeResult = _consumer.Consume(TimeSpan.FromMilliseconds(1000));

                    if (consumeResult == null) continue;

                    if (!consumeResult.IsPartitionEOF)
                    {
                        var kafkaMessage = CreateKafkaMessage(consumeResult);
                        await ProcessMessageAsync(kafkaMessage, cancellationToken);

                        if (!_options.EnableAutoCommit)
                        {
                            try
                            {
                                _consumer.Commit(consumeResult);
                            }
                            catch (KafkaException ex)
                            {
                                _logger.LogError(ex, "Failed to commit offset: {ErrorMessage}", ex.Message);
                            }
                        }
                    }
                }
                catch (ConsumeException ex) when (ex.Error.Code == ErrorCode.Local_Intr)
                {
                    _logger.LogInformation("Consumer stop requested via Kafka error code.");
                    break;
                }
                catch (ConsumeException ex) when (ex.Error.Code == ErrorCode.UnknownTopicOrPart)
                {
                    // MOST IMPORTANT PART: If the topic is not found, don't close the app, let it wait
                    _logger.LogWarning("Topic does not exist. Retrying in 5 seconds... Topic: {Topics}",
                        string.Join(", ", topics));
                    await Task.Delay(5000, cancellationToken);
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "Consume error: {ErrorMessage}", ex.Error.Reason);
                    await Task.Delay(1000, cancellationToken); // Short wait to prevent the error from recurring
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Unexpected error during consumption");
                    await Task.Delay(2000, cancellationToken);
                }
            }
        }
        finally
        {
            _consuming = false;
            _semaphore.Release();
        }
    }

    public async Task StartConsumingWithBatchProcessingAsync(IEnumerable<string> topics, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await _semaphore.WaitAsync(cancellationToken);
        try
        {
            if (_consuming)
            {
                _logger.LogWarning("Consumer is already running");
                return;
            }

            _consumer.Subscribe(topics);
            _consuming = true;
            _logger.LogInformation("Started consuming from topics: {Topics} with batch processing", string.Join(", ", topics));

            while (_consuming && !cancellationToken.IsCancellationRequested)
            {
                try
                {
                    // We ensure faster response to cancellationToken by adding a timeout
                    var consumeResult = _consumer.Consume(TimeSpan.FromMilliseconds(1000));

                    if (consumeResult == null) continue;

                    if (!consumeResult.IsPartitionEOF)
                    {
                        var kafkaMessage = CreateKafkaMessage(consumeResult);
                        await ProcessMessageAsync(kafkaMessage, cancellationToken);

                        if (!_options.EnableAutoCommit)
                        {
                            try
                            {
                                _consumer.Commit(consumeResult);
                            }
                            catch (KafkaException ex)
                            {
                                _logger.LogError(ex, "Failed to commit offset: {ErrorMessage}", ex.Message);
                            }
                        }
                    }
                }
                catch (ConsumeException ex) when (ex.Error.Code == ErrorCode.Local_Intr)
                {
                    _logger.LogInformation("Consumer stop requested via Kafka error code.");
                    break;
                }
                catch (ConsumeException ex) when (ex.Error.Code == ErrorCode.UnknownTopicOrPart)
                {
                    _logger.LogWarning("Topic does not exist. Retrying in 5 seconds... Topic: {Topics}",
                        string.Join(", ", topics));
                    await Task.Delay(5000, cancellationToken);
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "Consume error: {ErrorMessage}", ex.Error.Reason);
                    await Task.Delay(1000, cancellationToken); // Short wait to prevent the error from recurring
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Unexpected error during consumption");
                    await Task.Delay(2000, cancellationToken);
                }
            }
        }
        finally
        {
            _consuming = false;
            _semaphore.Release();
        }
    }

    public async Task SubscribeToPartitionsAsync(string topic, IEnumerable<int> partitions, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var topicPartitions = partitions.Select(p => new TopicPartition(topic, new Partition(p))).ToList();
        
        _consumer.Assign(topicPartitions);
        _consuming = true;

        _logger.LogInformation("Subscribed to specific partitions for topic: {Topic}, partitions: {Partitions}", 
            topic, string.Join(", ", partitions));

        // Start consuming from assigned partitions
        while (_consuming && !cancellationToken.IsCancellationRequested)
        {
            try
            {
                var consumeResult = _consumer.Consume(cancellationToken);

                if (consumeResult != null && !consumeResult.IsPartitionEOF)
                {
                    var kafkaMessage = CreateKafkaMessage(consumeResult);
                    
                    await ProcessMessageAsync(kafkaMessage, cancellationToken);

                    // Commit the offset after processing
                    if (!_options.EnableAutoCommit)
                    {
                        try
                        {
                            _consumer.Commit(consumeResult);
                        }
                        catch (KafkaException ex)
                        {
                            _logger.LogError(ex, "Failed to commit offset: {ErrorMessage}", ex.Message);
                        }
                    }
                }
            }
            catch (ConsumeException ex) when (ex.Error.Code == ErrorCode.Local_Intr)
            {
                _logger.LogInformation("Consumer cancellation requested");
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during partition-specific consumption");
                
                try
                {
                    await Task.Delay(100, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }
    }

    public bool IsConnected()
    {
        if (_disposed) return false;
        
        try
        {
            var assignment = _consumer.Assignment;
            return assignment != null && assignment.Count > 0;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Kafka connection test failed: {ErrorMessage}", ex.Message);
            return false;
        }
    }

    public async Task<long> GetPositionAsync(string topic, int partition)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var topicPartition = new TopicPartition(topic, new Partition(partition));
        var position = await Task.Run(() => _consumer.Position(topicPartition));
        return position.Value;
    }

    public async Task<long> GetCommittedOffsetAsync(string topic, int partition)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var topicPartition = new TopicPartition(topic, new Partition(partition));
        var offsets = await Task.Run(() => _consumer.Committed([topicPartition], TimeSpan.FromSeconds(5)));
        return offsets.FirstOrDefault()?.Offset.Value ?? -1;
    }

    public void PausePartitions(IEnumerable<string> topicPartitions)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var partitions = topicPartitions.Select(tp => 
        {
            var parts = tp.Split('-');
            return new TopicPartition(parts[0], new Partition(int.Parse(parts[1])));
        }).ToList();

        _consumer.Pause(partitions);
        _logger.LogInformation("Paused partitions: {Partitions}", string.Join(", ", topicPartitions));
    }

    public void ResumePartitions(IEnumerable<string> topicPartitions)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var partitions = topicPartitions.Select(tp => 
        {
            var parts = tp.Split('-');
            return new TopicPartition(parts[0], new Partition(int.Parse(parts[1])));
        }).ToList();

        _consumer.Resume(partitions);
        _logger.LogInformation("Resumed partitions: {Partitions}", string.Join(", ", topicPartitions));
    }

    private async Task ProcessMessageAsync(KafkaMessage message, CancellationToken cancellationToken)
    {
        using var scope = _scopeFactory.CreateScope();
        var processor = scope.ServiceProvider.GetRequiredService<IMessageProcessor>();

        try
        {
            await processor.ProcessAsync(message, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing message with key: {Key}", message.Key);
            
            // Handle the error with retry logic
            await HandleMessageProcessingErrorAsync(message, ex, cancellationToken);
        }
    }

    private async Task HandleMessageProcessingErrorAsync(KafkaMessage message, Exception originalException, CancellationToken cancellationToken)
    {
        // Extract retry count from message headers
        var retryCount = 0;
        if (message.Headers.TryGetValue("retryCount", out string? value))
        {
            _ = int.TryParse(value, out retryCount);
        }

        if (retryCount < _options.MaxRetriesOnProcessingFailure)
        {
            // Update message with retry information
            message.Headers["retryCount"] = (retryCount + 1).ToString();
            message.Headers["lastError"] = originalException.Message;
            message.Headers["retryTimestamp"] = DateTime.UtcNow.ToString("O");
            
            _logger.LogWarning("Retrying message processing for key: {Key}, attempt {Attempt}/{MaxRetries}", 
                message.Key, retryCount + 1, _options.MaxRetriesOnProcessingFailure);
            
            // Wait before retry
            await Task.Delay(_options.ProcessingRetryDelay, cancellationToken);
            
            // Retry processing
            using var scope = _scopeFactory.CreateScope();
            var processor = scope.ServiceProvider.GetRequiredService<IMessageProcessor>();
            await processor.ProcessAsync(message, cancellationToken);
        }
        else
        {
            _logger.LogError(originalException, "Message processing failed after {MaxRetries} retries for key: {Key}", 
                _options.MaxRetriesOnProcessingFailure, message.Key);
            
            // Send to dead letter queue if configured
            await SendToDeadLetterQueueAsync(message, originalException, cancellationToken);
            
            // Also call the error handler
            using var scope = _scopeFactory.CreateScope();
            var processor = scope.ServiceProvider.GetRequiredService<IMessageProcessor>();
            try
            {
                await processor.HandleErrorAsync(message, originalException, cancellationToken);
            }
            catch (Exception errorEx)
            {
                _logger.LogError(errorEx, "Error handling failed message with key: {Key}", message.Key);
            }
        }
    }

    private async Task SendToDeadLetterQueueAsync(KafkaMessage originalMessage, Exception exception, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(_options.DeadLetterQueueTopic))
        {
            _logger.LogWarning("No dead letter queue topic configured, skipping DLQ for message with key: {Key}", originalMessage.Key);
            return;
        }

        try
        {
            // Create a dead letter message with error details
            var deadLetterMessage = new KafkaMessage
            {
                Key = originalMessage.Key,
                Value = originalMessage.Value,
                Topic = _options.DeadLetterQueueTopic,
                Headers = new Dictionary<string, string>(originalMessage.Headers)
                {
                    ["originalTopic"] = originalMessage.Topic,
                    ["errorReason"] = exception.Message,
                    ["errorType"] = exception.GetType().Name,
                    ["errorTimestamp"] = DateTime.UtcNow.ToString("O"),
                    ["processedAt"] = DateTime.UtcNow.ToString("O")
                },
                CorrelationId = originalMessage.CorrelationId,
                MessageType = originalMessage.MessageType
            };

            using var scope = _scopeFactory.CreateScope();
            var producer = scope.ServiceProvider.GetRequiredService<IKafkaProducerService>();
            
            await producer.ProduceAsync(deadLetterMessage);
            
            _logger.LogInformation("Message sent to dead letter queue: {DLQTopic} for original key: {Key}", 
                _options.DeadLetterQueueTopic, originalMessage.Key);
        }
        catch (Exception dlqEx)
        {
            _logger.LogError(dlqEx, "Failed to send message to dead letter queue for key: {Key}", originalMessage.Key);
        }
    }

    private static KafkaMessage CreateKafkaMessage(ConsumeResult<string, string> consumeResult)
    {
        var headers = new Dictionary<string, string>();
        
        if (consumeResult.Message.Headers != null)
        {
            foreach (var header in consumeResult.Message.Headers)
            {
                var value = header.GetValueBytes();
                if (value != null)
                {
                    headers[header.Key] = Encoding.UTF8.GetString(value);
                }
            }
        }

        return new KafkaMessage
        {
            Key = consumeResult.Message.Key ?? string.Empty,
            Value = consumeResult.Message.Value ?? string.Empty,
            Headers = headers,
            Topic = consumeResult.Topic,
            Partition = consumeResult.Partition.Value,
            Offset = consumeResult.Offset.Value,
            Timestamp = consumeResult.Message.Timestamp.UtcDateTime
        };
    }

    private void OnConsumerError(IConsumer<string, string> consumer, Error error)
    {
        if (error.IsFatal)
        {
            _logger.LogCritical("Consumer fatal error: {Reason}", error.Reason);
            
            // Attempt to recreate consumer in case of fatal error
            RecreateConsumer();
        }
        else
        {
            _logger.LogWarning("Consumer error: {Reason}", error.Reason);
        }
    }

    private void OnConsumerLog(IConsumer<string, string> consumer, LogMessage message)
    {
        var level = message.Level switch
        {
            SyslogLevel.Emergency or SyslogLevel.Alert or SyslogLevel.Critical or SyslogLevel.Error => LogLevel.Error,
            SyslogLevel.Warning => LogLevel.Warning,
            SyslogLevel.Notice or SyslogLevel.Info => LogLevel.Information,
            SyslogLevel.Debug => LogLevel.Debug,
            _ => LogLevel.Trace
        };

        _logger.Log(level, "Kafka Consumer: {Facility} - {Message}", message.Facility, message.Message);
    }

    private void OnPartitionsAssigned(IConsumer<string, string> consumer, List<TopicPartition> partitions)
    {
        _logger.LogInformation("Partitions assigned: {Partitions}", 
            string.Join(", ", partitions.Select(p => $"{p.Topic}[{p.Partition}]")));
    }

    private void OnPartitionsRevoked(IConsumer<string, string> consumer, List<TopicPartitionOffset> partitions)
    {
        _logger.LogInformation("Partitions revoked: {Partitions}", 
            string.Join(", ", partitions.Select(p => $"{p.TopicPartition.Topic}[{p.TopicPartition.Partition}]")));
    }

    private void RecreateConsumer()
    {
        _semaphore.Wait();
        try
        {
            _logger.LogInformation("Recreating Kafka consumer due to error");
            
            _consumer?.Close();
            _consumer?.Dispose();
            _consumer = CreateConsumer();
            
            _logger.LogInformation("Kafka consumer recreated successfully");
        }
        finally
        {
            _semaphore.Release();
        }
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (!_disposed && disposing)
        {
            _semaphore.Wait();
            try
            {
                if (_consuming)
                {
                    _consuming = false;
                }
                
                _consumer?.Close();
                _consumer?.Dispose();
                _semaphore.Dispose();
            }
            finally
            {
                _disposed = true;
                _semaphore.Release();
            }
        }
    }
}
