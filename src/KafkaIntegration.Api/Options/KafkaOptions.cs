using System.ComponentModel.DataAnnotations;

namespace KafkaIntegration.Api.Options;

public class KafkaOptions
{
    public const string SectionName = "Kafka";


    [Required]
    public string BootstrapServers { get; set; } = string.Empty;

    [Required]
    public string ClientId { get; set; } = string.Empty;

    [Required]
    public string ConsumerGroupId { get; set; } = string.Empty;
    
    [Required] 
    public string DefaultTopic { get; set; } = string.Empty;
}
