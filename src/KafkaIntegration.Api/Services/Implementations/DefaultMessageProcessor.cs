using KafkaIntegration.Api.Models;

namespace KafkaIntegration.Api.Services.Implementations;

/// <summary>
/// Default message processor implementation that logs messages
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
            "Processing message - Topic: {Topic}, Partition: {Partition}, Offset: {Offset}, Key: {Key}, Value: {Value}",
            message.Topic,
            message.Partition,
            message.Offset,
            message.Key,
            message.Value);

        // In a real application, you would implement actual business logic here
        // For now, just await to make the method async
        await Task.CompletedTask;
    }

    public async Task HandleErrorAsync(KafkaMessage message, Exception exception, CancellationToken cancellationToken = default)
    {
        logger.LogError(
            exception,
            "Error processing message - Topic: {Topic}, Partition: {Partition}, Offset: {Offset}, Key: {Key}, Value: {Value}",
            message.Topic,
            message.Partition,
            message.Offset,
            message.Key,
            message.Value);

        // In a real application, you might send to a dead letter queue or error topic
        await Task.CompletedTask;
    }
}
