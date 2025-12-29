# Kafka Integration .NET API

A .NET 9 application that demonstrates integration with Apache Kafka for event-driven communication. This API provides endpoints for producing messages to Kafka topics and includes a background service for consuming messages.

## Features

- **Message Production**: Send messages to Kafka topics via REST API endpoints
- **Message Consumption**: Background service that continuously consumes messages from Kafka
- **Structured Message Support**: Send messages with explicit topic, key, value, and headers
- **Producer Management**: API endpoints to manage and flush the producer queue
- **Health Checks**: API endpoint to verify Kafka connectivity
- **Docker Support**: Complete Docker Compose setup with Kafka broker
- **Swagger Documentation**: API documentation via NSwag
- **Advanced Configuration**: Comprehensive Kafka settings for production use
- **Security Support**: SASL authentication and secure communication protocols
- **Robust Error Handling**: Proper disposal patterns and error recovery mechanisms
- **Configuration Validation**: Data annotation validation for Kafka options

## Technologies Used

- **.NET 9**: Latest .NET framework
- **Apache Kafka**: Distributed streaming platform
- **Confluent.Kafka**: .NET client library for Kafka
- **Docker & Docker Compose**: Containerization and orchestration
- **NSwag**: API documentation and OpenAPI generation
- **BackgroundService**: Asynchronous message consumption
- **IHostedService**: Managed background service for consumer lifecycle
- **Options Pattern**: Configuration management with validation
- **Health Checks**: Built-in health monitoring
- **Data Annotations**: Configuration validation

## Prerequisites

- Docker Desktop (with Docker Compose support)
- .NET 9 SDK (for local development)
- At least 4GB of RAM recommended for Docker containers

## Getting Started

### Using Docker Compose (Recommended)

The easiest way to run the application is using Docker Compose, which will start both the API and Kafka broker:

```bash
# Clone the repository
git clone <repository-url>
cd kafka-integration-dotnet

# Start the services
docker-compose up -d

# The API will be available at:
# - HTTP: http://localhost:8080
# - HTTPS: http://localhost:8081
```

### Local Development

For local development, you'll need a Kafka broker running separately:

```bash
# Run the application locally
dotnet run --project src/KafkaIntegration.Api/KafkaIntegration.Api.csproj
```

## Configuration

The application is configured through `appsettings.json`:

```json
{
  "Kafka": {
    "BootstrapServers": "kafka:29092",
    "ClientId": "kafka-dotnet-integration-demo",
    "ConsumerGroupId": "kafka-dotnet-consumer-group",
    "DefaultTopic": "dotnet-kafka-topic"
  }
}
```

### Configuration Options

**Connection Settings**
- `BootstrapServers`: Kafka broker address
- `ClientId`: Client identifier for the application
- `SaslUsername`: Username for SASL authentication (optional)
- `SaslPassword`: Password for SASL authentication (optional)
- `SecurityProtocol`: Security protocol for communication (PLAINTEXT, SASL_PLAINTEXT, SASL_SSL, SSL) - default: PLAINTEXT
- `SaslMechanism`: SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, GSSAPI, OAUTHBEARER) - default: PLAIN

**Producer Settings**
- `DefaultTopic`: Default topic for message production and consumption
- `MessageTimeoutMs`: Timeout for message sends in milliseconds - default: 30000
- `RequestTimeoutMs`: Timeout for requests in milliseconds - default: 30000
- `BatchNumMessages`: Maximum number of messages to batch together - default: 10000
- `LingerMs`: Delay in milliseconds to wait for more messages before sending - default: 5
- `QueueBufferingMaxMessages`: Maximum number of messages allowed in the producer queue - default: 100000
- `QueueBufferingMaxKbytes`: Maximum total size of messages allowed in the producer queue - default: 1048576
- `MessageSendMaxRetries`: Maximum number of retries for failed message sends - default: 3
- `CompressionType`: Compression algorithm to use (none, gzip, snappy, lz4, zstd) - default: snappy

**Consumer Settings**
- `ConsumerGroupId`: Consumer group for message consumption
- `SessionTimeoutMs`: Client group session timeout - default: 10000
- `AutoOffsetReset`: Action when no initial offset is found (1=earliest, 2=latest) - default: 1
- `EnableAutoCommit`: Whether to automatically commit offsets - default: false
- `MaxPollIntervalMs`: Maximum delay between message consumption - default: 300000
- `MaxPartitionFetchBytes`: Maximum bytes to fetch from a single partition - default: 1048576
- `FetchMinBytes`: Minimum bytes to fetch in a single request - default: 1
- `FetchWaitMaxMs`: Maximum time to wait for FetchMinBytes - default: 500

**Common Settings**
- `SocketTimeoutMs`: Socket timeout in milliseconds - default: 60000
- `SocketConnectionSetupTimeoutMs`: Timeout for socket connection setup - default: 30000
- `ReconnectBackoffMs`: Initial delay between reconnection attempts - default: 100
- `ReconnectBackoffMaxMs`: Maximum delay between reconnection attempts - default: 10000

**Health Check Settings**
- `HealthCheckTimeout`: Timeout for health checks - default: 00:00:05 (5 seconds)

**Serialization Settings**
- `MessageEncoding`: Character encoding for messages - default: utf-8
- `SerializerType`: Serialization format (json, avro, protobuf) - default: json

## API Endpoints

### Send Message to Default Topic
```
POST /api/kafka/send
```

Send a message to the default Kafka topic.

**Request Body**:
```json
{
  "key": "optional-message-key",
  "value": "message-content",
  "headers": {
    "header1": "value1",
    "header2": "value2"
  }
}
```

**Example**:
```bash
curl -X POST http://localhost:8080/api/kafka/send \
  -H "Content-Type: application/json" \
  -d '{"key": "test-key", "value": "Hello Kafka!", "headers": {"source": "api"}}'
```

**Response**:
```json
{
  "status": "Message sent successfully",
  "topic": "dotnet-kafka-topic",
  "partition": 0,
  "offset": 5,
  "timestamp": "2023-01-01T00:00:00.000Z"
}
```

### Send Message to Specific Topic
```
POST /api/kafka/send/{topic}
```

Send a message to a specific Kafka topic.

**Example**:
```bash
curl -X POST http://localhost:8080/api/kafka/send/my-topic \
  -H "Content-Type: application/json" \
  -d '{"key": "test-key", "value": "Hello Kafka!"}'
```

### Send Structured Message
```
POST /api/kafka/send-structured
```

Send a structured message with explicit topic, key, value, and headers.

**Request Body**:
```json
{
  "topic": "my-topic",
  "key": "message-key",
  "value": "message-content",
  "headers": {
    "header1": "value1",
    "header2": "value2"
  }
}
```

**Example**:
```bash
curl -X POST http://localhost:8080/api/kafka/send-structured \
  -H "Content-Type: application/json" \
  -d '{"topic": "my-topic", "key": "test-key", "value": "Structured message", "headers": {"priority": "high"}}'
```

### Flush Producer
```
POST /api/kafka/flush
```

Force flush all pending messages in the producer queue.

**Query Parameters**:
- `timeoutSeconds`: Timeout in seconds for flush operation (default: 10)

**Example**:
```bash
curl -X POST http://localhost:8080/api/kafka/flush?timeoutSeconds=15
```

**Response**:
```json
{
  "status": "Flush completed",
  "timeoutSeconds": 15
}
```

### Health Check
```
GET /api/kafka/health
```

Check the health status of the Kafka connection.

**Example**:
```bash
curl http://localhost:8080/api/kafka/health
```

**Healthy Response**:
```json
{
  "status": "Healthy",
  "message": "Kafka producer connection is active"
}
```

**Unhealthy Response**:
```json
{
  "status": "Unhealthy",
  "message": "Kafka producer connection failed"
}
```

## Architecture

### Components

1. **KafkaController**: REST API endpoints for message production, health checks, and producer management
2. **KafkaProducerService**: Service for producing messages to Kafka with advanced configuration and error handling
3. **KafkaConsumerService**: Service for consuming messages from Kafka with advanced configuration and error handling
4. **KafkaConsumerHostedService**: Background service that starts the consumer and handles lifecycle events
5. **MessageDto**: Data transfer object for basic message requests
6. **KafkaMessage**: Model for structured message requests with explicit topic, key, value, and headers
7. **KafkaOptions**: Configuration options with validation for all Kafka settings
8. **KafkaHealthCheck**: Health check implementation to verify Kafka connectivity
9. **IMessageProcessor**: Interface for message processing logic with default implementation

### Message Flow

1. **Production**: API receives POST requests and sends messages to Kafka with support for headers and structured messages
2. **Consumption**: Background service continuously consumes messages from Kafka with proper error handling and recovery
3. **Processing**: Consumed messages are processed through the message processor with topic, partition, and offset information
4. **Commitment**: Consumer commits offsets after successful message processing with proper resource management
5. **Error Handling**: Services implement proper disposal patterns and error recovery mechanisms
6. **Health Monitoring**: Health check endpoint verifies producer connectivity and overall system status

## Docker Compose Services

The project includes a complete Docker Compose setup:

- **kafkaintegration.api**: The .NET API application
- **kafka**: Apache Kafka broker (Confluent Platform 7.5.0)

### Ports

- API HTTP: 8080
- API HTTPS: 8081
- Kafka: 9092 (external), 29092 (internal)

## Development

### Running Tests

```bash
# Run unit tests
dotnet test

# Run with coverage
dotnet test --collect:"XPlat Code Coverage"
```

### Building

```bash
# Build the solution
dotnet build

# Build Docker images
docker-compose build
```

### Environment-specific Configuration

For different environments, you can override the configuration using:

- Environment variables
- User secrets (`dotnet user-secrets`)
- Custom configuration files (`appsettings.Production.json`)

## Troubleshooting

### Common Issues

1. **Kafka Connection Issues**:
   - Ensure Kafka broker is running
   - Check `BootstrapServers` configuration
   - Verify network connectivity between containers

2. **Consumer Not Receiving Messages**:
   - Check consumer group configuration
   - Verify topic exists
   - Review log output for errors

3. **Docker Resource Issues**:
   - Increase Docker memory allocation
   - Check system resource availability

### Logging

The application logs important events including:
- Message production details
- Message consumption details
- Connection status
- Error conditions

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.