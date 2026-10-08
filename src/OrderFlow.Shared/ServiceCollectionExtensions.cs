using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace OrderFlow.Shared;

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the shared Kafka plumbing: settings bound from the "Kafka" section,
    /// the admin service, the singleton producer, and the startup topic initializer.
    /// </summary>
    public static IServiceCollection AddOrderFlowInfrastructure(
        this IServiceCollection services, IConfiguration configuration)
    {
        services.AddOptions<KafkaSettings>()
            .Bind(configuration.GetSection(KafkaSettings.SectionName))
            .ValidateOnStart();

        // Fails startup with a readable message if the topic list or retry budget is wrong.
        services.AddSingleton<IValidateOptions<KafkaSettings>, KafkaSettingsValidator>();

        services.AddSingleton<KafkaAdminService>();
        services.AddSingleton<IEventProducer, KafkaEventProducer>();
        services.AddHostedService<KafkaTopicInitializer>();

        return services;
    }
}
