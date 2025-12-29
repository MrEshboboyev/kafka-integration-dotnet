using System.ComponentModel.DataAnnotations;

namespace KafkaIntegration.Api.Models;

/// <summary>
/// Internal representation of a Kafka message with comprehensive metadata
/// </summary>
public class KafkaMessage
{
    [Required]
    public string Key { get; set; } = string.Empty;

    [Required]
    public string Value { get; set; } = string.Empty;

    public Dictionary<string, string> Headers { get; set; } = [];
    
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
    
    public string Topic { get; set; } = string.Empty;
    
    public int? Partition { get; set; }
    
    public long? Offset { get; set; }
    
    public string? CorrelationId { get; set; }
    
    public string? MessageType { get; set; }
}
