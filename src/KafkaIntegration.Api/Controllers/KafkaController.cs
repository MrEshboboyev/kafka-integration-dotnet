using KafkaIntegration.Api.Models;
using KafkaIntegration.Api.Options;
using KafkaIntegration.Api.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace KafkaIntegration.Api.Controllers;

[ApiController]
[Route("api/kafka")]
public class KafkaController(
    IKafkaProducerService producerService,
    IOptions<KafkaOptions> kafkaOptions,
    ILogger<KafkaController> logger
) : ControllerBase
{
    [HttpPost("send")]
    public async Task<IActionResult> SendMessage([FromBody] MessageDto messageDto)
    {
        if (!ModelState.IsValid)
        {
            return BadRequest(ModelState);
        }

        try
        {
            var topic = kafkaOptions.Value.DefaultTopic;

            var result = await producerService.ProduceAsync(
                topic,
                messageDto.Key ?? Guid.NewGuid().ToString(),
                messageDto.Value,
                messageDto.Headers);

            return Ok(new
            {
                Status = "Message sent successfully",
                result.Topic,
                Partition = result.Partition.Value,
                Offset = result.Offset.Value,
                Timestamp = DateTime.UtcNow
            });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error sending message with key: {MessageKey}", messageDto.Key);
            return StatusCode(500, new { Message = "Failed to send message", Error = ex.Message });
        }
    }

    [HttpPost("send/{topic}")]
    public async Task<IActionResult> SendMessageToTopic(string topic, [FromBody] MessageDto messageDto)
    {
        if (!ModelState.IsValid)
        {
            return BadRequest(ModelState);
        }

        try
        {
            var result = await producerService.ProduceAsync(
                topic,
                messageDto.Key ?? Guid.NewGuid().ToString(),
                messageDto.Value,
                messageDto.Headers);

            return Ok(new
            {
                Status = "Message sent successfully",
                result.Topic,
                Partition = result.Partition.Value,
                Offset = result.Offset.Value,
                Timestamp = DateTime.UtcNow
            });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error sending message to topic {Topic} with key: {MessageKey}", topic, messageDto.Key);
            return StatusCode(500, new { Message = "Failed to send message", Error = ex.Message });
        }
    }

    [HttpPost("send-structured")]
    public async Task<IActionResult> SendStructuredMessage([FromBody] KafkaMessage kafkaMessage)
    {
        if (!ModelState.IsValid)
        {
            return BadRequest(ModelState);
        }

        try
        {
            var result = await producerService.ProduceAsync(kafkaMessage);

            return Ok(new
            {
                Status = "Message sent successfully",
                result.Topic,
                Partition = result.Partition.Value,
                Offset = result.Offset.Value,
                Timestamp = DateTime.UtcNow
            });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error sending structured message to topic {Topic} with key: {MessageKey}", kafkaMessage.Topic, kafkaMessage.Key);
            return StatusCode(500, new { Message = "Failed to send message", Error = ex.Message });
        }
    }

    [HttpGet("health")]
    public IActionResult CheckHealth()
    {
        bool isProducerConnected = producerService.IsConnected();

        if (isProducerConnected)
        {
            return Ok(new { Status = "Healthy", Message = "Kafka producer connection is active" });
        }

        return StatusCode(503, new { Status = "Unhealthy", Message = "Kafka producer connection failed" });
    }

    [HttpPost("flush")]
    public async Task<IActionResult> FlushProducer([FromQuery] int timeoutSeconds = 10)
    {
        try
        {
            await producerService.FlushAsync(TimeSpan.FromSeconds(timeoutSeconds));
            return Ok(new { Status = "Flush completed", TimeoutSeconds = timeoutSeconds });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error flushing producer");
            return StatusCode(500, new { Message = "Failed to flush producer", Error = ex.Message });
        }
    }
}
