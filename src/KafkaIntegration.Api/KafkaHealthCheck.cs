using Confluent.Kafka;
using KafkaIntegration.Api.Options;
using KafkaIntegration.Api.Services;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;

namespace KafkaIntegration.Api;

public class KafkaHealthCheck(
    IKafkaProducerService producerService,
    IKafkaConsumerService consumerService,
    IOptions<KafkaOptions> kafkaOptions,
    ILogger<KafkaHealthCheck> logger
) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        var data = new Dictionary<string, object>();
        
        try
        {
            // Check producer connection
            var isProducerHealthy = producerService.IsConnected();
            data["producer"] = isProducerHealthy ? "healthy" : "unhealthy";
            
            // Check consumer connection
            var isConsumerHealthy = consumerService.IsConnected();
            data["consumer"] = isConsumerHealthy ? "healthy" : "unhealthy";
            
            // Additional health checks using AdminClient
            var adminClientHealthy = await CheckAdminClientHealthAsync(cancellationToken);
            data["admin_client"] = adminClientHealthy ? "healthy" : "unhealthy";
            
            // Check if default topic exists
            var topicExists = await CheckTopicExistsAsync(kafkaOptions.Value.DefaultTopic, cancellationToken);
            data["default_topic"] = topicExists ? "exists" : "missing";
            
            // Overall health status
            if (isProducerHealthy && isConsumerHealthy && adminClientHealthy && topicExists)
            {
                return HealthCheckResult.Healthy("Kafka connection is fully healthy");
            }
            else if (isProducerHealthy || isConsumerHealthy || adminClientHealthy)
            {
                return HealthCheckResult.Degraded("Kafka connection is partially healthy");
            }
            else
            {
                return HealthCheckResult.Unhealthy("Kafka connection is unhealthy");
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Health check failed");
            var errorData = new Dictionary<string, object> { ["error"] = ex.Message };
            return HealthCheckResult.Unhealthy("Health check failed", ex);
        }
    }
    
    private async Task<bool> CheckAdminClientHealthAsync(CancellationToken cancellationToken)
    {
        try
        {
            using var adminClient = new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = kafkaOptions.Value.BootstrapServers,
                SecurityProtocol = string.IsNullOrEmpty(kafkaOptions.Value.SecurityProtocol) 
                    ? SecurityProtocol.Plaintext 
                    : Enum.Parse<SecurityProtocol>(kafkaOptions.Value.SecurityProtocol, ignoreCase: true),
                SaslUsername = kafkaOptions.Value.SaslUsername,
                SaslPassword = kafkaOptions.Value.SaslPassword,
                SaslMechanism = string.IsNullOrEmpty(kafkaOptions.Value.SaslMechanism) 
                    ? SaslMechanism.Plain 
                    : Enum.Parse<SaslMechanism>(kafkaOptions.Value.SaslMechanism, ignoreCase: true)
            }).Build();

            // Test admin client by getting cluster metadata
            var metadata = await Task.Run(() => adminClient.GetMetadata(TimeSpan.FromSeconds(5)), cancellationToken);
            
            return metadata.Brokers.Count > 0;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Admin client health check failed");
            return false;
        }
    }
    
    private async Task<bool> CheckTopicExistsAsync(string topicName, CancellationToken cancellationToken)
    {
        try
        {
            using var adminClient = new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = kafkaOptions.Value.BootstrapServers,
                SecurityProtocol = string.IsNullOrEmpty(kafkaOptions.Value.SecurityProtocol) 
                    ? SecurityProtocol.Plaintext 
                    : Enum.Parse<SecurityProtocol>(kafkaOptions.Value.SecurityProtocol, ignoreCase: true),
                SaslUsername = kafkaOptions.Value.SaslUsername,
                SaslPassword = kafkaOptions.Value.SaslPassword,
                SaslMechanism = string.IsNullOrEmpty(kafkaOptions.Value.SaslMechanism) 
                    ? SaslMechanism.Plain 
                    : Enum.Parse<SaslMechanism>(kafkaOptions.Value.SaslMechanism, ignoreCase: true)
            }).Build();

            var metadata = await Task.Run(() => adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(5)), cancellationToken);
            
            return metadata.Topics.Any(t => t.Topic == topicName);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Topic existence check failed for topic: {TopicName}", topicName);
            return false;
        }
    }
}
