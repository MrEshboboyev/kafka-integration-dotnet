using Confluent.Kafka;

namespace KafkaIntegration.Api.Services;

public interface IKafkaProducerService
{
    Task<DeliveryResult<string, string>> ProduceAsync(string topic, string key, string value);
    bool IsConnected();
}
