using KafkaIntegration.Api.Models;
using System.Text.Json;

namespace KafkaIntegration.Api.Services.Implementations;

/// <summary>
/// Default message processor implementation that handles messages based on type
/// </summary>
public class DefaultMessageProcessor(
    ILogger<DefaultMessageProcessor> logger
) : IMessageProcessor
{
    public async Task ProcessAsync(
        KafkaMessage message,
        CancellationToken cancellationToken = default)
    {
        logger.LogInformation(
            "Processing message - Topic: {Topic}, Partition: {Partition}, Offset: {Offset}, Key: {Key}, Type: {MessageType}",
            message.Topic,
            message.Partition,
            message.Offset,
            message.Key,
            message.MessageType ?? "unknown");

        try
        {
            // Process based on message type
            switch (message.MessageType?.ToLowerInvariant())
            {
                case "json":
                    await ProcessJsonMessageAsync(message, cancellationToken);
                    break;
                case "text":
                    await ProcessTextMessageAsync(message, cancellationToken);
                    break;
                default:
                    await ProcessGenericMessageAsync(message, cancellationToken);
                    break;
            }
        }
        catch (Exception ex)
        {
            logger.LogError(
                ex,
                "Error processing message - Topic: {Topic}, Key: {Key}, MessageType: {MessageType}",
                message.Topic,
                message.Key,
                message.MessageType ?? "unknown");
            
            throw; // Re-throw to trigger error handling
        }
    }

    private async Task ProcessJsonMessageAsync(KafkaMessage message, CancellationToken cancellationToken)
    {
        try
        {
            var jsonDocument = JsonDocument.Parse(message.Value);
            logger.LogInformation("Processed JSON message with {PropertyCount} properties", jsonDocument.RootElement.EnumerateObject().Count());
        }
        catch (JsonException ex)
        {
            logger.LogWarning(ex, "Invalid JSON in message with key: {Key}", message.Key);
            throw;
        }
        
        await Task.CompletedTask;
    }
    
    private async Task ProcessTextMessageAsync(KafkaMessage message, CancellationToken cancellationToken)
    {
        logger.LogInformation("Processed text message with length: {Length}", message.Value.Length);
        await Task.CompletedTask;
    }
    
    private async Task ProcessGenericMessageAsync(KafkaMessage message, CancellationToken cancellationToken)
    {
        logger.LogInformation("Processed generic message with length: {Length}", message.Value.Length);
        await Task.CompletedTask;
    }

    public async Task HandleErrorAsync(KafkaMessage message, Exception exception, CancellationToken cancellationToken = default)
    {
        logger.LogError(
            exception,
            "Error processing message - Topic: {Topic}, Partition: {Partition}, Offset: {Offset}, Key: {Key}, MessageType: {MessageType}",
            message.Topic,
            message.Partition,
            message.Offset,
            message.Key,
            message.MessageType ?? "unknown");

        // In a real application, you might send to a dead letter queue or error topic
        await Task.CompletedTask;
    }
}
