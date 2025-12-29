using Confluent.Kafka;
using KafkaIntegration.Api.Models;

namespace KafkaIntegration.Api.Services;

public interface IKafkaProducerService
{
    /// <summary>
    /// Produces a message to the specified topic
    /// </summary>
    Task<DeliveryResult<string, string>> ProduceAsync(string topic, string key, string value);
    
    /// <summary>
    /// Produces a message with headers to the specified topic
    /// </summary>
    Task<DeliveryResult<string, string>> ProduceAsync(string topic, string key, string value, Dictionary<string, string> headers);
    
    /// <summary>
    /// Produces a KafkaMessage object to the specified topic
    /// </summary>
    Task<DeliveryResult<string, string>> ProduceAsync(KafkaMessage message);
    
    /// <summary>
    /// Produces a typed message to the specified topic
    /// </summary>
    Task<DeliveryResult<string, string>> ProduceAsync<T>(string topic, string key, T value, Dictionary<string, string>? headers = null) where T : class;
    
    /// <summary>
    /// Produces a message with retry logic
    /// </summary>
    Task<DeliveryResult<string, string>> ProduceWithRetryAsync(string topic, string key, string value, Dictionary<string, string>? headers = null, int maxRetries = 3);
    
    /// <summary>
    /// Checks if the producer is connected to Kafka
    /// </summary>
    bool IsConnected();
    
    /// <summary>
    /// Flushes any pending messages
    /// </summary>
    Task FlushAsync(TimeSpan timeout);
}
