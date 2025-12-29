using KafkaIntegration.Api;
using KafkaIntegration.Api.Options;
using KafkaIntegration.Api.Services;
using KafkaIntegration.Api.Services.Implementations;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddNSwagSwagger();

// Register Kafka options with validation
builder.Services.AddOptions<KafkaOptions>()
    .Bind(builder.Configuration.GetSection(KafkaOptions.SectionName))
    .ValidateDataAnnotations()
    .ValidateOnStart();

// Register Kafka services
builder.Services.AddSingleton<IKafkaProducerService, KafkaProducerService>();
builder.Services.AddSingleton<IKafkaConsumerService, KafkaConsumerService>();
builder.Services.AddScoped<IMessageProcessor, DefaultMessageProcessor>();

// Register hosted service to start consumer
builder.Services.AddHostedService<KafkaConsumerHostedService>();

// Add health checks
builder.Services.AddHealthChecks()
    .AddCheck<KafkaHealthCheck>("kafka");

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseNSwagSwagger();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

// Map health check endpoint
app.MapHealthChecks("/health");

app.Run();
