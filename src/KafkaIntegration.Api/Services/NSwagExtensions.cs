namespace KafkaIntegration.Api.Services;

public static class NSwagExtensions
{
    public static IServiceCollection AddNSwagSwagger(this IServiceCollection services)
    {
        services.AddEndpointsApiExplorer();
        services.AddSwaggerDocument(config =>
            config.PostProcess = (settings =>
            {
                settings.Info.Title = "Product Management API";
                settings.Info.Version = "v1";
                settings.Info.Description = "API for managing products in the Product Management system";
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
