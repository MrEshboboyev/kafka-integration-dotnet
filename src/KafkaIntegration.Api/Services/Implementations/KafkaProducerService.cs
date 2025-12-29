using Confluent.Kafka;
using KafkaIntegration.Api.Models;
using KafkaIntegration.Api.Options;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace KafkaIntegration.Api.Services.Implementations;

public class KafkaProducerService : IKafkaProducerService, IAsyncDisposable, IDisposable
{
    private IProducer<string, string> _producer;
    private readonly ILogger<KafkaProducerService> _logger;
    private readonly KafkaOptions _options;
    private readonly SemaphoreSlim _semaphore = new(1, 1);
    private bool _disposed = false;
    private readonly JsonSerializerOptions _jsonSerializerOptions;

    public KafkaProducerService(IOptions<KafkaOptions> kafkaOptions, ILogger<KafkaProducerService> logger)
    {
        _logger = logger;
        _options = kafkaOptions.Value;
        _jsonSerializerOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };

        _producer = CreateProducer();
    }

    private IProducer<string, string> CreateProducer()
    {
        var config = new ProducerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            ClientId = _options.ClientId,
            Acks = Acks.All,
            MessageTimeoutMs = _options.MessageTimeoutMs,
            RequestTimeoutMs = _options.RequestTimeoutMs,
            BatchNumMessages = _options.BatchNumMessages,
            LingerMs = _options.LingerMs,
            QueueBufferingMaxMessages = _options.QueueBufferingMaxMessages,
            QueueBufferingMaxKbytes = _options.QueueBufferingMaxKbytes,
            MessageSendMaxRetries = _options.MessageSendMaxRetries,
            RetryBackoffMs = _options.RetryBackoffMs,
            MaxInFlight = (_options.EnableIdempotence == true)
                ? Math.Min(_options.MaxInFlight, 5)
                : _options.MaxInFlight,
            EnableIdempotence = _options.EnableIdempotence,
            SecurityProtocol = string.IsNullOrEmpty(_options.SecurityProtocol) 
                ? SecurityProtocol.Plaintext 
                : Enum.Parse<SecurityProtocol>(_options.SecurityProtocol, ignoreCase: true),
            SaslUsername = _options.SaslUsername,
            SaslPassword = _options.SaslPassword,
            SaslMechanism = string.IsNullOrEmpty(_options.SaslMechanism) 
                ? SaslMechanism.Plain 
                : Enum.Parse<SaslMechanism>(_options.SaslMechanism, ignoreCase: true),
            CompressionType = string.IsNullOrEmpty(_options.CompressionType) 
                ? CompressionType.Snappy 
                : Enum.Parse<CompressionType>(_options.CompressionType, ignoreCase: true)
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

        return new ProducerBuilder<string, string>(config)
            .SetErrorHandler(OnProducerError)
            .SetLogHandler(OnProducerLog)
            .Build();
    }

    public async Task<DeliveryResult<string, string>> ProduceAsync(string topic, string key, string value)
    {
        var headers = new Dictionary<string, string>();
        return await ProduceAsync(topic, key, value, headers);
    }

    public async Task<DeliveryResult<string, string>> ProduceAsync(string topic, string key, string value, Dictionary<string, string> headers)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        try
        {
            var message = new Message<string, string>
            {
                Key = key,
                Value = value,
                Headers = CreateKafkaHeaders(headers)
            };

            var result = await _producer.ProduceAsync(topic, message);
            
            _logger.LogInformation(
                "Message delivered: Topic={Topic}, Partition=[{Partition}], Offset={Offset}, Key={Key}", 
                result.Topic, 
                result.Partition, 
                result.Offset, 
                key);
            
            return result;
        }
        catch (ProduceException<string, string> ex)
        {
            _logger.LogError(
                ex, 
                "Failed to deliver message: Topic={Topic}, Key={Key}, Error={Error}", 
                topic, 
                key, 
                ex.Error.Reason);
            
            throw;
        }
    }

    public async Task<DeliveryResult<string, string>> ProduceAsync(KafkaMessage message)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        // Add message metadata to headers
        var headers = new Dictionary<string, string>(message.Headers)
        {
            ["timestamp"] = message.Timestamp.ToString("O"),
            ["correlationId"] = message.CorrelationId ?? Guid.NewGuid().ToString(),
            ["messageType"] = message.MessageType ?? "unknown",
            ["source"] = _options.ClientId
        };
        
        if (!string.IsNullOrEmpty(message.CorrelationId))
            headers["correlationId"] = message.CorrelationId;
            
        if (!string.IsNullOrEmpty(message.MessageType))
            headers["messageType"] = message.MessageType;

        var result = await ProduceAsync(message.Topic, message.Key, message.Value, headers);
        
        // Update message with delivery information
        message.Partition = (int)result.Partition;
        message.Offset = (long)result.Offset;

        return result;
    }

    public async Task<DeliveryResult<string, string>> ProduceAsync<T>(
        string topic,
        string key,
        T value,
        Dictionary<string, string>? headers = null) where T : class
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        try
        {
            string serializedValue = JsonSerializer.Serialize(value, _jsonSerializerOptions);
            
            var messageHeaders = headers ?? [];
            messageHeaders["contentType"] = "application/json";
            messageHeaders["serializedType"] = typeof(T).Name;

            var message = new Message<string, string>
            {
                Key = key,
                Value = serializedValue,
                Headers = CreateKafkaHeaders(messageHeaders)
            };

            var result = await _producer.ProduceAsync(topic, message);
            
            _logger.LogInformation(
                "Message delivered: Topic={Topic}, Partition=[{Partition}], Offset={Offset}, Key={Key}, Type={Type}", 
                result.Topic, 
                result.Partition, 
                result.Offset, 
                key,
                typeof(T).Name);
            
            return result;
        }
        catch (ProduceException<string, string> ex)
        {
            _logger.LogError(
                ex, 
                "Failed to deliver message: Topic={Topic}, Key={Key}, Type={Type}, Error={Error}", 
                topic, 
                key, 
                typeof(T).Name, 
                ex.Error.Reason);
            
            throw;
        }
    }

    public async Task<DeliveryResult<string, string>> ProduceWithRetryAsync(string topic, string key, string value, Dictionary<string, string>? headers = null, int maxRetries = 3)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var message = new Message<string, string>
        {
            Key = key,
            Value = value,
            Headers = CreateKafkaHeaders(headers ?? new Dictionary<string, string>())
        };

        for (int attempt = 0; attempt <= maxRetries; attempt++)
        {
            try
            {
                var result = await _producer.ProduceAsync(topic, message);
                
                _logger.LogInformation(
                    "Message delivered: Topic={Topic}, Partition=[{Partition}], Offset={Offset}, Key={Key}, Attempt={Attempt}", 
                    result.Topic, 
                    result.Partition, 
                    result.Offset, 
                    key,
                    attempt + 1);
                
                return result;
            }
            catch (ProduceException<string, string> ex) when (attempt < maxRetries)
            {
                _logger.LogWarning(
                    ex, 
                    "Failed to deliver message: Topic={Topic}, Key={Key}, Attempt={Attempt}/{MaxRetries}, Error={Error}. Retrying...", 
                    topic, 
                    key, 
                    attempt + 1, 
                    maxRetries,
                    ex.Error.Reason);
                
                await Task.Delay(TimeSpan.FromSeconds(Math.Pow(2, attempt + 1))); // Exponential backoff
            }
            catch (ProduceException<string, string> ex)
            {
                _logger.LogError(
                    ex, 
                    "Failed to deliver message after {MaxRetries} retries: Topic={Topic}, Key={Key}, Error={Error}", 
                    maxRetries,
                    topic, 
                    key, 
                    ex.Error.Reason);
                
                throw;
            }
        }
        
        throw new InvalidOperationException("Retry logic failed to execute properly");
    }

    public bool IsConnected()
    {
        if (_disposed) return false;
        
        try
        {
            using var adminClient = new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = _options.BootstrapServers,
                SecurityProtocol = string.IsNullOrEmpty(_options.SecurityProtocol) 
                    ? SecurityProtocol.Plaintext 
                    : Enum.Parse<SecurityProtocol>(_options.SecurityProtocol, ignoreCase: true),
                SaslUsername = _options.SaslUsername,
                SaslPassword = _options.SaslPassword,
                SaslMechanism = string.IsNullOrEmpty(_options.SaslMechanism) 
                    ? SaslMechanism.Plain 
                    : Enum.Parse<SaslMechanism>(_options.SaslMechanism, ignoreCase: true)
            }).Build();

            var metadata = adminClient.GetMetadata(_options.DefaultTopic, _options.HealthCheckTimeout);
            
            return metadata.Brokers.Count > 0;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Kafka connection test failed: {ErrorMessage}", ex.Message);
            return false;
        }
    }

    public async Task FlushAsync(TimeSpan timeout)
    {
        if (_disposed) return;
        
        await _semaphore.WaitAsync();
        try
        {
            await Task.Run(() => _producer.Flush(timeout));
        }
        finally
        {
            _semaphore.Release();
        }
    }

    private static Headers CreateKafkaHeaders(Dictionary<string, string> headers)
    {
        var kafkaHeaders = new Headers();
        foreach (var header in headers)
        {
            if (!string.IsNullOrEmpty(header.Value))
            {
                kafkaHeaders.Add(header.Key, System.Text.Encoding.UTF8.GetBytes(header.Value));
            }
        }
        return kafkaHeaders;
    }

    private void OnProducerError(IProducer<string, string> producer, Error error)
    {
        if (error.IsFatal)
        {
            _logger.LogCritical("Producer fatal error: {Reason}", error.Reason);
            
            // Attempt to recreate producer in case of fatal error
            RecreateProducer();
        }
        else
        {
            _logger.LogWarning("Producer error: {Reason}", error.Reason);
        }
    }

    private void OnProducerLog(IProducer<string, string> producer, LogMessage message)
    {
        var level = message.Level switch
        {
            SyslogLevel.Emergency or SyslogLevel.Alert or SyslogLevel.Critical or SyslogLevel.Error => LogLevel.Error,
            SyslogLevel.Warning => LogLevel.Warning,
            SyslogLevel.Notice or SyslogLevel.Info => LogLevel.Information,
            SyslogLevel.Debug => LogLevel.Debug,
            _ => LogLevel.Trace
        };

        _logger.Log(level, "Kafka Producer: {Facility} - {Message}", message.Facility, message.Message);
    }

    private void RecreateProducer()
    {
        _semaphore.Wait();
        try
        {
            _logger.LogInformation("Recreating Kafka producer due to error");
            
            _producer?.Dispose();
            _producer = CreateProducer();
            
            _logger.LogInformation("Kafka producer recreated successfully");
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
                _producer?.Flush(TimeSpan.FromSeconds(10));
                _producer?.Dispose();
                _semaphore.Dispose();
            }
            finally
            {
                _disposed = true;
                _semaphore.Release();
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        await DisposeAsync(true);
        GC.SuppressFinalize(this);
    }

    protected virtual async ValueTask DisposeAsync(bool disposing)
    {
        if (!_disposed && disposing)
        {
            await _semaphore.WaitAsync();
            try
            {
                await Task.Run(() => _producer?.Flush(TimeSpan.FromSeconds(10)));
                _producer?.Dispose();
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
