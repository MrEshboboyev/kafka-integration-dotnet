namespace KafkaIntegration.Api.Services;

public class KafkaConsumerHostedService(
    IKafkaConsumerService consumerService,
    IConfiguration configuration,
    ILogger<KafkaConsumerHostedService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var topic = configuration["Kafka:DefaultTopic"];
        logger.LogInformation($"Starting Kafka consumer for topic: {topic}");

        await consumerService.StartConsumingAsync(topic, stoppingToken);
    }
}