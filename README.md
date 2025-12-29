# Kafka Integration .NET API

A .NET 9 application that demonstrates integration with Apache Kafka for event-driven communication. This API provides endpoints for producing messages to Kafka topics and includes a background service for consuming messages.

## Features

- **Message Production**: Send messages to Kafka topics via REST API endpoints
- **Message Consumption**: Background service that continuously consumes messages from Kafka
- **Health Checks**: API endpoint to verify Kafka connectivity
- **Docker Support**: Complete Docker Compose setup with Kafka broker
- **Swagger Documentation**: API documentation via NSwag
- **Configuration**: Externalized configuration for Kafka settings

## Technologies Used

- **.NET 9**: Latest .NET framework
- **Apache Kafka**: Distributed streaming platform
- **Confluent.Kafka**: .NET client library for Kafka
- **Docker & Docker Compose**: Containerization and orchestration
- **NSwag**: API documentation and OpenAPI generation
- **BackgroundService**: Asynchronous message consumption

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

- `BootstrapServers`: Kafka broker address
- `ClientId`: Client identifier for the application
- `ConsumerGroupId`: Consumer group for message consumption
- `DefaultTopic`: Default topic for message production and consumption

## API Endpoints

### Send Message
```
POST /api/kafka/send
```

Send a message to the default Kafka topic.

**Request Body**:
```json
{
  "key": "optional-message-key",
  "value": "message-content"
}
```

**Example**:
```bash
curl -X POST http://localhost:8080/api/kafka/send \
  -H "Content-Type: application/json" \
  -d '{"key": "test-key", "value": "Hello Kafka!"}'
```

**Response**:
```json
{
  "status": "Message sent successfully",
  "topic": "dotnet-kafka-topic",
  "partition": 0,
  "offset": 5
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
  "message": "Kafka connection is active"
}
```

**Unhealthy Response**:
```json
{
  "status": "Unhealthy",
  "message": "Kafka connection failed"
}
```

## Architecture

### Components

1. **KafkaController**: REST API endpoints for message production and health checks
2. **KafkaProducerService**: Service for producing messages to Kafka
3. **KafkaConsumerService**: Service for consuming messages from Kafka
4. **KafkaConsumerHostedService**: Background service that starts the consumer
5. **MessageDto**: Data transfer object for message requests

### Message Flow

1. **Production**: API receives POST requests and sends messages to Kafka
2. **Consumption**: Background service continuously consumes messages from Kafka
3. **Processing**: Consumed messages are logged with topic, partition, and offset information
4. **Commitment**: Consumer commits offsets after successful message processing

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