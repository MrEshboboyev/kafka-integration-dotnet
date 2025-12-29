using System.ComponentModel.DataAnnotations;

namespace KafkaIntegration.Api.Models;

public class MessageDto
{
    [Required]
    [StringLength(256, ErrorMessage = "Key cannot exceed 256 characters")]
    public string Key { get; set; } = string.Empty;

    [Required]
    [StringLength(10000, ErrorMessage = "Value cannot exceed 10,000 characters")]
    public string Value { get; set; } = string.Empty;

    public Dictionary<string, string> Headers { get; set; } = new Dictionary<string, string>();
    
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
}
