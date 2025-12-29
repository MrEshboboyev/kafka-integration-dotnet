using KafkaIntegration.Api;
using KafkaIntegration.Api.Options;
using KafkaIntegration.Api.Security;
using KafkaIntegration.Api.Services;
using KafkaIntegration.Api.Services.Implementations;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddNSwagSwagger();

// Register Kafka options with validation
builder.Services.AddOptions<KafkaOptions>()
    .Bind(builder.Configuration.GetSection(KafkaOptions.SectionName))
    .ValidateDataAnnotations()
    .ValidateOnStart();

// Register Kafka services with proper lifetimes
builder.Services.AddSingleton<IKafkaProducerService, KafkaProducerService>();
builder.Services.AddSingleton<IKafkaConsumerService, KafkaConsumerService>();
builder.Services.AddScoped<IMessageProcessor, DefaultMessageProcessor>();

// Register hosted service to start consumer
builder.Services.AddHostedService<KafkaConsumerHostedService>();

// Add health checks with detailed configuration
builder.Services.AddHealthChecks()
    .AddCheck<KafkaHealthCheck>("kafka", timeout: TimeSpan.FromSeconds(10));

// Configure authentication
var apiKey = builder.Configuration["ApiKey"];
if (!string.IsNullOrEmpty(apiKey))
{
    builder.Services.AddAuthentication("ApiKey")
        .AddScheme<ApiKeyAuthenticationOptions, ApiKeyAuthenticationHandler>("ApiKey", options =>
        {
            options.ApiKey = apiKey;
        });
}

// Configure JSON serialization globally
builder.Services.Configure<Microsoft.AspNetCore.Mvc.JsonOptions>(options =>
{
    options.JsonSerializerOptions.PropertyNamingPolicy = System.Text.Json.JsonNamingPolicy.CamelCase;
    options.JsonSerializerOptions.WriteIndented = false;
});

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseNSwagSwagger();
}

app.UseHttpsRedirection();

// Use authentication if configured
if (!string.IsNullOrEmpty(builder.Configuration["ApiKey"]))
{
    app.UseAuthentication();
}
app.UseAuthorization();

app.MapControllers();

// Map health check endpoint with detailed response
app.MapHealthChecks("/health", new HealthCheckOptions
{
    ResponseWriter = async (context, report) =>
    {
        context.Response.ContentType = "application/json";
        
        var response = new
        {
            Status = report.Status.ToString(),
            Checks = report.Entries.Select(e => new
            {
                Name = e.Key,
                Status = e.Value.Status.ToString(),
                e.Value.Description,
                e.Value.Duration,
                e.Value.Data
            }),
            report.TotalDuration
        };
        
        var json = System.Text.Json.JsonSerializer.Serialize(response);
        await context.Response.WriteAsync(json);
    }
});

app.Run();
