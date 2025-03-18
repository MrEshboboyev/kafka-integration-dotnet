namespace KafkaIntegration.Api.Services;

public interface IKafkaConsumerService
{
    Task StartConsumingAsync(string topic, CancellationToken cancellationToken);
    bool IsConnected();
}
