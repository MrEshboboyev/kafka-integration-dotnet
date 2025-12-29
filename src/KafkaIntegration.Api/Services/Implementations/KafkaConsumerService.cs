using Confluent.Kafka;
using KafkaIntegration.Api.Models;
using KafkaIntegration.Api.Options;
using Microsoft.Extensions.Options;
using System.Text;

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

    public KafkaConsumerService(
        IOptions<KafkaOptions> kafkaOptions, 
        ILogger<KafkaConsumerService> logger,
        IServiceScopeFactory scopeFactory)
    {
        _logger = logger;
        _scopeFactory = scopeFactory;
        _options = kafkaOptions.Value;

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
            SessionTimeoutMs = _options.SessionTimeoutMs,
            MaxPollIntervalMs = _options.MaxPollIntervalMs,
            MaxPartitionFetchBytes = _options.MaxPartitionFetchBytes,
            FetchMinBytes = _options.FetchMinBytes,
            FetchWaitMaxMs = _options.FetchWaitMaxMs,
            SocketTimeoutMs = _options.SocketTimeoutMs,
            SocketConnectionSetupTimeoutMs = _options.SocketConnectionSetupTimeoutMs,
            ReconnectBackoffMs = _options.ReconnectBackoffMs,
            ReconnectBackoffMaxMs = _options.ReconnectBackoffMaxMs,
            EnablePartitionEof = true
        };

        // Add security configuration if provided
        if (!string.IsNullOrEmpty(_options.SecurityProtocol))
        {
            config.SecurityProtocol = Enum.Parse<SecurityProtocol>(_options.SecurityProtocol, ignoreCase: true);
            
            if (!string.IsNullOrEmpty(_options.SaslUsername) && !string.IsNullOrEmpty(_options.SaslPassword))
            {
                config.SaslUsername = _options.SaslUsername;
                config.SaslPassword = _options.SaslPassword;
                
                if (!string.IsNullOrEmpty(_options.SaslMechanism))
                {
                    config.SaslMechanism = Enum.Parse<SaslMechanism>(_options.SaslMechanism, ignoreCase: true);
                }
            }
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
                    _logger.LogWarning("Topic hali mavjud emas. 5 soniyadan keyin qayta uriniladi... Topic: {Topics}",
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
            
            try
            {
                await processor.HandleErrorAsync(message, ex, cancellationToken);
            }
            catch (Exception errorEx)
            {
                _logger.LogError(errorEx, "Error handling failed message with key: {Key}", message.Key);
            }
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
