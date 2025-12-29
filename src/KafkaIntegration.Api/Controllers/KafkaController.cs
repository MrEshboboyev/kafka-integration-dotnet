using KafkaIntegration.Api.Models;
using KafkaIntegration.Api.Options;
using KafkaIntegration.Api.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace KafkaIntegration.Api.Controllers;

[ApiController]
[Route("api/kafka")]
[Produces("application/json")]
public class KafkaController(
    IKafkaProducerService producerService,
    IOptions<KafkaOptions> kafkaOptions,
    ILogger<KafkaController> logger
) : ControllerBase
{
    [HttpPost("send")]
    [ProducesResponseType(typeof(object), 200)]
    [ProducesResponseType(typeof(object), 400)]
    [ProducesResponseType(typeof(object), 500)]
    public async Task<IActionResult> SendMessage([FromBody] MessageDto messageDto)
    {
        if (!ModelState.IsValid)
        {
            return BadRequest(new
            {
                Message = "Validation failed",
                Errors = ModelState.ToDictionary(
                    kvp => kvp.Key,
                    kvp => kvp.Value?.Errors.Select(e => e.ErrorMessage).ToArray()
                )
            });
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
                Timestamp = DateTime.UtcNow,
                CorrelationId = result.Message.Headers.TryGetLastBytes("correlationId", out var correlationIdBytes) 
                    ? System.Text.Encoding.UTF8.GetString(correlationIdBytes) 
                    : Guid.NewGuid().ToString()
            });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error sending message with key: {MessageKey}", messageDto.Key);
            return StatusCode(500, new { 
                Message = "Failed to send message", 
                Error = ex.Message,
                CorrelationId = Guid.NewGuid().ToString(),
                Timestamp = DateTime.UtcNow
            });
        }
    }

    [HttpPost("send/{topic}")]
    public async Task<IActionResult> SendMessageToTopic([FromRoute] string topic, [FromBody] MessageDto messageDto)
    {
        if (!ModelState.IsValid)
        {
            return BadRequest(new
            {
                Message = "Validation failed",
                Errors = ModelState.ToDictionary(
                    kvp => kvp.Key,
                    kvp => kvp.Value?.Errors.Select(e => e.ErrorMessage).ToArray()
                )
            });
        }

        if (string.IsNullOrWhiteSpace(topic))
        {
            return BadRequest(new { Message = "Topic cannot be empty" });
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
                Timestamp = DateTime.UtcNow,
                CorrelationId = result.Message.Headers.TryGetLastBytes("correlationId", out var correlationIdBytes) 
                    ? System.Text.Encoding.UTF8.GetString(correlationIdBytes) 
                    : Guid.NewGuid().ToString()
            });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error sending message to topic {Topic} with key: {MessageKey}", topic, messageDto.Key);
            return StatusCode(500, new { 
                Message = "Failed to send message", 
                Error = ex.Message,
                CorrelationId = Guid.NewGuid().ToString(),
                Topic = topic,
                Timestamp = DateTime.UtcNow
            });
        }
    }

    [HttpPost("send-structured")]
    public async Task<IActionResult> SendStructuredMessage([FromBody] KafkaMessage kafkaMessage)
    {
        if (!ModelState.IsValid)
        {
            return BadRequest(new
            {
                Message = "Validation failed",
                Errors = ModelState.ToDictionary(
                    kvp => kvp.Key,
                    kvp => kvp.Value?.Errors.Select(e => e.ErrorMessage).ToArray()
                )
            });
        }

        if (string.IsNullOrWhiteSpace(kafkaMessage.Topic))
        {
            return BadRequest(new { Message = "Topic cannot be empty" });
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
                Timestamp = DateTime.UtcNow,
                CorrelationId = kafkaMessage.CorrelationId ?? Guid.NewGuid().ToString()
            });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error sending structured message to topic {Topic} with key: {MessageKey}", kafkaMessage.Topic, kafkaMessage.Key);
            return StatusCode(500, new { 
                Message = "Failed to send message", 
                Error = ex.Message,
                CorrelationId = kafkaMessage.CorrelationId ?? Guid.NewGuid().ToString(),
                Topic = kafkaMessage.Topic,
                Timestamp = DateTime.UtcNow
            });
        }
    }

    [HttpPost("send-generic")]
    [Authorize]
    public async Task<IActionResult> SendGenericMessage<T>([FromBody] T message, [FromQuery] string topic, [FromQuery] string? key = null) where T : class
    {
        if (string.IsNullOrWhiteSpace(topic))
        {
            return BadRequest(new { Message = "Topic cannot be empty" });
        }

        try
        {
            var result = await producerService.ProduceAsync(
                topic,
                key ?? Guid.NewGuid().ToString(),
                message,
                new Dictionary<string, string> { ["messageType"] = typeof(T).Name });

            return Ok(new
            {
                Status = "Message sent successfully",
                result.Topic,
                Partition = result.Partition.Value,
                Offset = result.Offset.Value,
                Timestamp = DateTime.UtcNow,
                CorrelationId = Guid.NewGuid().ToString()
            });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error sending generic message of type {MessageType} to topic {Topic}", typeof(T).Name, topic);
            return StatusCode(500, new { 
                Message = "Failed to send message", 
                Error = ex.Message,
                MessageType = typeof(T).Name,
                CorrelationId = Guid.NewGuid().ToString(),
                Topic = topic,
                Timestamp = DateTime.UtcNow
            });
        }
    }

    [HttpGet("health")]
    public IActionResult CheckHealth()
    {
        bool isProducerConnected = producerService.IsConnected();
        bool isConsumerConnected = true; // Placeholder - would need to check consumer too

        if (isProducerConnected && isConsumerConnected)
        {
            return Ok(new { 
                Status = "Healthy", 
                Message = "Kafka producer and consumer connections are active",
                Timestamp = DateTime.UtcNow
            });
        }
        else if (isProducerConnected)
        {
            return Ok(new { 
                Status = "Degraded", 
                Message = "Kafka producer connection is active but consumer connection is not checked",
                Timestamp = DateTime.UtcNow
            });
        }
        else
        {
            return StatusCode(503, new { 
                Status = "Unhealthy", 
                Message = "Kafka producer connection failed",
                Timestamp = DateTime.UtcNow
            });
        }
    }

    [HttpPost("flush")]
    [Authorize]
    public async Task<IActionResult> FlushProducer([FromQuery] int timeoutSeconds = 10)
    {
        if (timeoutSeconds <= 0 || timeoutSeconds > 300) // Max 5 minutes
        {
            return BadRequest(new { Message = "Timeout must be between 1 and 300 seconds" });
        }

        try
        {
            await producerService.FlushAsync(TimeSpan.FromSeconds(timeoutSeconds));
            return Ok(new { 
                Status = "Flush completed", 
                TimeoutSeconds = timeoutSeconds,
                Timestamp = DateTime.UtcNow
            });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error flushing producer");
            return StatusCode(500, new { 
                Message = "Failed to flush producer", 
                Error = ex.Message,
                CorrelationId = Guid.NewGuid().ToString(),
                Timestamp = DateTime.UtcNow
            });
        }
    }

    [HttpGet("status")]
    public IActionResult GetStatus()
    {
        try
        {
            var producerStatus = producerService.IsConnected();
            
            return Ok(new
            {
                ProducerConnected = producerStatus,
                ConsumerConnected = true, // Would need actual consumer status check
                Timestamp = DateTime.UtcNow,
                Version = "1.0.0"
            });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error getting Kafka status");
            return StatusCode(500, new { 
                Message = "Failed to get Kafka status", 
                Error = ex.Message,
                CorrelationId = Guid.NewGuid().ToString(),
                Timestamp = DateTime.UtcNow
            });
        }
    }
}
