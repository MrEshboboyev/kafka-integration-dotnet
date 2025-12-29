using System.ComponentModel.DataAnnotations;

namespace KafkaIntegration.Api.Options;

public class KafkaOptions
{
    public const string SectionName = "Kafka";

    // Connection settings
    [Required]
    public string BootstrapServers { get; set; } = string.Empty;

    [Required]
    public string ClientId { get; set; } = string.Empty;
    
    public string? SaslUsername { get; set; }
    public string? SaslPassword { get; set; }
    public string? SecurityProtocol { get; set; } = "PLAINTEXT"; // PLAINTEXT, SASL_PLAINTEXT, SASL_SSL, SSL
    public string? SaslMechanism { get; set; } = "PLAIN"; // PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, GSSAPI, OAUTHBEARER

    // Producer settings
    [Required] 
    public string DefaultTopic { get; set; } = string.Empty;
    
    public int MessageTimeoutMs { get; set; } = 30000;
    public int RequestTimeoutMs { get; set; } = 30000;
    public int BatchNumMessages { get; set; } = 10000;
    public int LingerMs { get; set; } = 5;
    public int QueueBufferingMaxMessages { get; set; } = 100000;
    public int QueueBufferingMaxKbytes { get; set; } = 1048576;
    public int MessageSendMaxRetries { get; set; } = 3;
    public string? CompressionType { get; set; } = "Snappy"; // none, gzip, snappy, lz4, zstd
    
    // Consumer settings
    [Required]
    public string ConsumerGroupId { get; set; } = string.Empty;
    
    public int SessionTimeoutMs { get; set; } = 10000;
    public int AutoOffsetReset { get; set; } = 1; // 1 = earliest, 2 = latest
    public bool EnableAutoCommit { get; set; } = false;
    public int MaxPollIntervalMs { get; set; } = 300000;
    public int MaxPartitionFetchBytes { get; set; } = 1048576;
    public int FetchMinBytes { get; set; } = 1;
    public int FetchWaitMaxMs { get; set; } = 500;
    
    // Common settings
    public int SocketTimeoutMs { get; set; } = 60000;
    public int SocketConnectionSetupTimeoutMs { get; set; } = 30000;
    public int ReconnectBackoffMs { get; set; } = 100;
    public int ReconnectBackoffMaxMs { get; set; } = 10000;
    
    // Health check settings
    public TimeSpan HealthCheckTimeout { get; set; } = TimeSpan.FromSeconds(5);
    
    // Serialization settings
    public string MessageEncoding { get; set; } = "utf-8";
    public string SerializerType { get; set; } = "json"; // json, avro, protobuf
}
