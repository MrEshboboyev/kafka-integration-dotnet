using Confluent.Kafka;
using KafkaIntegration.Api.Options;
using Microsoft.Extensions.Options;

namespace KafkaIntegration.Api.Services.Implementations;

public class KafkaProducerService : IKafkaProducerService, IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly ILogger<KafkaProducerService> _logger;
    private readonly KafkaOptions _options;

    public KafkaProducerService(IOptions<KafkaOptions> kafkaOptions, ILogger<KafkaProducerService> logger)
    {
        _logger = logger;
        _options = kafkaOptions.Value;

        var config = new ProducerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            ClientId = _options.ClientId,
            Acks = Acks.All
        };

        _producer = new ProducerBuilder<string, string>(config).Build();
    }

    public async Task<DeliveryResult<string, string>> ProduceAsync(string topic, string key, string value)
    {
        try
        {
            var message = new Message<string, string>
            {
                Key = key,
                Value = value
            };

            var result = await _producer.ProduceAsync(topic, message);
            _logger.LogInformation($"Message delivered: {result.Topic} [{result.Partition}] @ {result.Offset}");
            return result;
        }
        catch (ProduceException<string, string> ex)
        {
            _logger.LogError($"Failed to deliver message: {ex.Message}");
            throw;
        }
    }

    public bool IsConnected()
    {
        try
        {
            using var adminClient = new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = _options.BootstrapServers
            }).Build();

            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(2));

            return metadata.Brokers.Count > 0;
        }
        catch (Exception ex)
        {
            _logger.LogError($"Kafka connection test failed: {ex.Message}");
            return false;
        }
    }

    public void Dispose()
    {
        _producer?.Flush(TimeSpan.FromSeconds(10));
        _producer?.Dispose();
    }
}
