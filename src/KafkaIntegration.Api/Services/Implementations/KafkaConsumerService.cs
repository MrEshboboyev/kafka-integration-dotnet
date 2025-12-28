using Confluent.Kafka;
using KafkaIntegration.Api.Options;
using Microsoft.Extensions.Options;

namespace KafkaIntegration.Api.Services.Implementations;

public class KafkaConsumerService : IKafkaConsumerService, IDisposable
{
    private readonly IConsumer<string, string> _consumer;
    private readonly ILogger<KafkaConsumerService> _logger;
    private bool _consuming = false;

    public KafkaConsumerService(IOptions<KafkaOptions> kafkaOptions, ILogger<KafkaConsumerService> logger)
    {
        _logger = logger;

        var config = new ConsumerConfig
        {
            BootstrapServers = kafkaOptions.Value.BootstrapServers,
            GroupId = kafkaOptions.Value.ConsumerGroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        _consumer = new ConsumerBuilder<string, string>(config).Build();
    }

    public async Task StartConsumingAsync(string topic, CancellationToken cancellationToken)
    {
        _consumer.Subscribe(topic);
        _consuming = true;

        _logger.LogInformation($"Started consuming from topic: {topic}");

        try
        {
            while (_consuming && !cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = _consumer.Consume(cancellationToken);

                    if (consumeResult != null)
                    {
                        _logger.LogInformation(
                            $"Received message: {consumeResult.Topic} [{consumeResult.Partition}] @ {consumeResult.Offset}: " +
                            $"Key = {consumeResult.Message.Key}, Value = {consumeResult.Message.Value}");

                        // Process message here

                        // Commit the offset after processing
                        _consumer.Commit(consumeResult);
                    }
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError($"Consume error: {ex.Message}");
                }

                await Task.Delay(10, cancellationToken); // Small delay to prevent CPU spinning
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Consumer was stopped gracefully");
        }
        finally
        {
            _consumer.Close();
            _consuming = false;
        }
    }

    public bool IsConnected()
    {
        try
        {
            using var adminClient = new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = _consumer.MemberId
            }).Build();

            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(5));
            return metadata.OriginatingBrokerId >= 0;
        }
        catch (Exception ex)
        {
            _logger.LogError($"Kafka connection test failed: {ex.Message}");
            return false;
        }
    }

    public void Dispose()
    {
        if (_consuming)
        {
            _consuming = false;
        }
        _consumer?.Dispose();
    }
}
