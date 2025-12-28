using KafkaIntegration.Api.Models;
using KafkaIntegration.Api.Services;
using Microsoft.AspNetCore.Mvc;

namespace KafkaIntegration.Api.Controllers;

[ApiController]
[Route("api/[controller]")]
public class KafkaController(
    IKafkaProducerService producerService,
    IConfiguration configuration,
    ILogger<KafkaController> logger
) : ControllerBase
{
    [HttpPost("send")]
    public async Task<IActionResult> SendMessage([FromBody] MessageDto messageDto)
    {
        try
        {
            var topic = configuration["Kafka:DefaultTopic"];

            var result = await producerService.ProduceAsync(
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
            logger.LogError($"Error sending message: {ex.Message}");
            return StatusCode(500, $"Failed to send message: {ex.Message}");
        }
    }

    [HttpGet("health")]
    public IActionResult CheckHealth()
    {
        bool isProducerConnected = producerService.IsConnected();

        if (isProducerConnected)
        {
            return Ok(new { Status = "Healthy", Message = "Kafka connection is active" });
        }

        return StatusCode(503, new { Status = "Unhealthy", Message = "Kafka connection failed" });
    }
}
