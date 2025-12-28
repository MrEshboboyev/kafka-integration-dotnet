using Confluent.Kafka;

namespace KafkaIntegration.Api.Services.Implementations;

public class KafkaProducerService : IKafkaProducerService, IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly ILogger<KafkaProducerService> _logger;

    public KafkaProducerService(IConfiguration configuration, ILogger<KafkaProducerService> logger)
    {
        _logger = logger;

        var config = new ProducerConfig
        {
            BootstrapServers = configuration["Kafka:BootstrapServers"],
            ClientId = configuration["Kafka:ClientId"] ?? "kafka-dotnet-producer",
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
            // Create a temporary admin client to test connectivity
            using var adminClient = new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = _producer.Name
            }).Build();

            // Get metadata about the cluster to verify connection
            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(5));
            return metadata.OriginatingBrokerId >= 0;
        }
        catch (Exception ex)
        {
            _logger.LogError($"Kafka connection test failed: {ex.Message}");
            return false;
        }
    }

    public void Dispose()
    {
        _producer?.Dispose();
    }
}
