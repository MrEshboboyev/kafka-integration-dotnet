using KafkaIntegration.Api.Services;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace KafkaIntegration.Api;

public class KafkaHealthCheck(
    IKafkaProducerService producerService,
    IKafkaConsumerService consumerService,
    ILogger<KafkaHealthCheck> logger
) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        try
        {
            // Check producer connection
            var isProducerHealthy = producerService.IsConnected();
            
            // Check consumer connection
            var isConsumerHealthy = consumerService.IsConnected();
            
            if (isProducerHealthy && isConsumerHealthy)
            {
                return HealthCheckResult.Healthy("Kafka connection is healthy");
            }
            else if (!isProducerHealthy && !isConsumerHealthy)
            {
                return HealthCheckResult.Unhealthy("Both producer and consumer are unhealthy");
            }
            else if (!isProducerHealthy)
            {
                return HealthCheckResult.Degraded("Producer is unhealthy");
            }
            else
            {
                return HealthCheckResult.Degraded("Consumer is unhealthy");
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Health check failed");
            return HealthCheckResult.Unhealthy("Health check failed", ex);
        }
    }
}
