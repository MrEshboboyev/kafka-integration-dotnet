using KafkaIntegration.Api.Options;
using Microsoft.Extensions.Options;

namespace KafkaIntegration.Api.Services;

public class KafkaConsumerHostedService(
    IKafkaConsumerService consumerService,
    IOptions<KafkaOptions> kafkaOptions,
    ILogger<KafkaConsumerHostedService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var topic = kafkaOptions.Value.DefaultTopic;
        logger.LogInformation("Starting Kafka consumer for topic: {Topic}", topic);

        await consumerService.StartConsumingAsync(topic, stoppingToken);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Stopping Kafka consumer hosted service");
        await base.StopAsync(cancellationToken);
    }
}
