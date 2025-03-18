using KafkaIntegration.Api.Services;
using Microsoft.AspNetCore.Mvc;

namespace KafkaIntegration.Api.Controllers;

[ApiController]
[Route("api/[controller]")]
public class KafkaController : ControllerBase
{
    private readonly IKafkaProducerService _producerService;
    private readonly IConfiguration _configuration;
    private readonly ILogger<KafkaController> _logger;

    public KafkaController(
        IKafkaProducerService producerService,
        IConfiguration configuration,
        ILogger<KafkaController> logger)
    {
        _producerService = producerService;
        _configuration = configuration;
        _logger = logger;
    }

    [HttpPost("send")]
    public async Task<IActionResult> SendMessage([FromBody] MessageDto messageDto)
    {
        try
        {
            var topic = _configuration["Kafka:DefaultTopic"];

            var result = await _producerService.ProduceAsync(
                topic,
                messageDto.Key ?? Guid.NewGuid().ToString(),
                messageDto.Value);

            return Ok(new
            {
                Status = "Message sent successfully",
                result.Topic,
                Partition = result.Partition.Value,
                Offset = result.Offset.Value
            });
        }
        catch (Exception ex)
        {
            _logger.LogError($"Error sending message: {ex.Message}");
            return StatusCode(500, $"Failed to send message: {ex.Message}");
        }
    }

    [HttpGet("health")]
    public IActionResult CheckHealth()
    {
        bool isProducerConnected = _producerService.IsConnected();

        if (isProducerConnected)
        {
            return Ok(new { Status = "Healthy", Message = "Kafka connection is active" });
        }

        return StatusCode(503, new { Status = "Unhealthy", Message = "Kafka connection failed" });
    }
}

public class MessageDto
{
    public string Key { get; set; }
    public string Value { get; set; }
}