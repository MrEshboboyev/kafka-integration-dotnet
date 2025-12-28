namespace KafkaIntegration.Api;

public static class NSwagExtensions
{
    public static IServiceCollection AddNSwagSwagger(this IServiceCollection services)
    {
        services.AddEndpointsApiExplorer();
        services.AddSwaggerDocument(config =>
            config.PostProcess = (settings =>
            {
                settings.Info.Title = "Kafka API";
                settings.Info.Version = "v1";
                settings.Info.Description = "API for Kafka Demo";
            }));

        return services;
    }

    public static IApplicationBuilder UseNSwagSwagger(this IApplicationBuilder app)
    {
        app.UseOpenApi();
        app.UseSwaggerUi();

        return app;
    }
}
