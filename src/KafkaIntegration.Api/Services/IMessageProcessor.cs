using KafkaIntegration.Api.Models;

namespace KafkaIntegration.Api.Services;

/// <summary>
/// Interface for processing consumed Kafka messages
/// </summary>
public interface IMessageProcessor
{
    /// <summary>
    /// Processes a Kafka message
    /// </summary>
    Task ProcessAsync(KafkaMessage message, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Handles message processing errors
    /// </summary>
    Task HandleErrorAsync(KafkaMessage message, Exception exception, CancellationToken cancellationToken = default);
}
