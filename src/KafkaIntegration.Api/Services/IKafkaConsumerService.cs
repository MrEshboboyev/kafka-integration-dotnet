namespace KafkaIntegration.Api.Services;

public interface IKafkaConsumerService
{
    /// <summary>
    /// Starts consuming messages from the specified topic
    /// </summary>
    Task StartConsumingAsync(string topic, CancellationToken cancellationToken);
    
    /// <summary>
    /// Starts consuming messages from multiple topics
    /// </summary>
    Task StartConsumingAsync(IEnumerable<string> topics, CancellationToken cancellationToken);
    
    /// <summary>
    /// Starts consuming messages with batch processing
    /// </summary>
    Task StartConsumingWithBatchProcessingAsync(IEnumerable<string> topics, CancellationToken cancellationToken);
    
    /// <summary>
    /// Subscribe to specific partitions of a topic
    /// </summary>
    Task SubscribeToPartitionsAsync(string topic, IEnumerable<int> partitions, CancellationToken cancellationToken);
    
    /// <summary>
    /// Checks if the consumer is connected to Kafka
    /// </summary>
    bool IsConnected();
    
    /// <summary>
    /// Gets the current consumer position for a partition
    /// </summary>
    Task<long> GetPositionAsync(string topic, int partition);
    
    /// <summary>
    /// Gets the current committed offset for a partition
    /// </summary>
    Task<long> GetCommittedOffsetAsync(string topic, int partition);
    
    /// <summary>
    /// Pauses consumption from specified partitions
    /// </summary>
    void PausePartitions(IEnumerable<string> topicPartitions);
    
    /// <summary>
    /// Resumes consumption from specified partitions
    /// </summary>
    void ResumePartitions(IEnumerable<string> topicPartitions);
}
