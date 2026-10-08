using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace OrderFlow.Shared;

/// <summary>
/// Runs before any other hosted service: waits for the broker, then provisions every topic
/// listed in the <c>Kafka:Topics</c> configuration array. Registered first so the consumer
/// never subscribes to a topic that does not exist yet.
/// </summary>
public sealed class KafkaTopicInitializer(
    KafkaAdminService admin,
    IOptions<KafkaSettings> settings,
    ILogger<KafkaTopicInitializer> logger) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var topics = settings.Value.Topics;

        logger.LogInformation(
            "Kafka configuration | bootstrapServers={BootstrapServers} topics={Count} [{Topics}]",
            settings.Value.BootstrapServers,
            topics.Count,
            string.Join(", ", topics.Select(t =>
                $"{t.Name}(p={t.ResolvePartitions(settings.Value)}{(t.Subscribe ? "" : ", no-subscribe")})")));

        logger.LogInformation("Waiting for Kafka broker...");
        await admin.WaitForBrokerAsync(ct: cancellationToken);
        await admin.EnsureTopicsExistAsync(topics, cancellationToken);
        logger.LogInformation("Kafka topic provisioning complete");
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
