using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace OrderFlow.Shared;

/// <summary>
/// Provisions topics at startup and waits for the broker to become reachable.
/// The wait matters in Docker: containers can start before the broker finishes
/// electing a controller, and a hard failure there would crash the service.
/// </summary>
public sealed class KafkaAdminService(IOptions<KafkaSettings> settings, ILogger<KafkaAdminService> logger)
{
    private readonly KafkaSettings _settings = settings.Value;

    /// <summary>Polls broker metadata until the cluster answers or the attempts run out.</summary>
    public async Task WaitForBrokerAsync(int maxAttempts = 30, int delayMs = 2_000, CancellationToken ct = default)
    {
        using var admin = BuildAdminClient();

        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                var metadata = admin.GetMetadata(TimeSpan.FromSeconds(5));
                if (metadata.Brokers.Count > 0)
                {
                    logger.LogInformation(
                        "Kafka reachable at {BootstrapServers} | brokers={BrokerCount} clusterId={ClusterId}",
                        _settings.BootstrapServers, metadata.Brokers.Count, metadata.OriginatingBrokerName);
                    return;
                }
            }
            catch (KafkaException ex)
            {
                logger.LogWarning(
                    "Kafka not ready (attempt {Attempt}/{Max}) at {BootstrapServers}: {Reason}",
                    attempt, maxAttempts, _settings.BootstrapServers, ex.Error.Reason);
            }

            await Task.Delay(delayMs, ct);
        }

        throw new InvalidOperationException(
            $"Kafka at '{_settings.BootstrapServers}' was unreachable after {maxAttempts} attempts. " +
            "Is the broker running? Try: docker compose up -d kafka");
    }

    /// <summary>
    /// Creates any configured topic that does not exist yet. Existing topics are left
    /// untouched — Kafka cannot reduce a partition count, and increasing one changes how
    /// keys hash to partitions, so growing a live topic is a deliberate operation rather
    /// than something startup should do silently.
    /// </summary>
    public async Task EnsureTopicsExistAsync(
        IEnumerable<TopicSettings> topics, CancellationToken ct = default)
    {
        using var admin = BuildAdminClient();

        var existing = admin.GetMetadata(TimeSpan.FromSeconds(10))
            .Topics.ToDictionary(t => t.Topic, t => t.Partitions.Count, StringComparer.Ordinal);

        var configured = topics.ToList();

        // Warn when configuration asks for more partitions than a pre-existing topic has:
        // the setting is silently ineffective until the topic is recreated or altered.
        foreach (var topic in configured.Where(t => existing.ContainsKey(t.Name)))
        {
            var wanted = topic.ResolvePartitions(_settings);
            var actual = existing[topic.Name];

            if (wanted != actual)
            {
                logger.LogWarning(
                    "Topic '{Topic}' already exists with {Actual} partition(s) but configuration asks for " +
                    "{Wanted}. Existing topics are not altered — delete the topic or use kafka-topics --alter.",
                    topic.Name, actual, wanted);
            }
        }

        var missing = configured.Where(t => !existing.ContainsKey(t.Name)).ToList();

        if (missing.Count == 0)
        {
            logger.LogInformation("All {Count} configured topics already exist: {Topics}",
                configured.Count, string.Join(", ", configured.Select(t => t.Name)));
            return;
        }

        var specs = missing.Select(t => new TopicSpecification
        {
            Name = t.Name,
            NumPartitions = t.ResolvePartitions(_settings),
            ReplicationFactor = t.ResolveReplicationFactor(_settings)
        }).ToList();

        try
        {
            await admin.CreateTopicsAsync(specs);
            foreach (var spec in specs)
                logger.LogInformation(
                    "Topic created | name={Topic} partitions={Partitions} replicationFactor={Replication}",
                    spec.Name, spec.NumPartitions, spec.ReplicationFactor);
        }
        catch (CreateTopicsException ex)
        {
            // Two services racing to provision the same topics is normal and benign.
            foreach (var result in ex.Results)
            {
                if (result.Error.Code == ErrorCode.TopicAlreadyExists)
                    logger.LogDebug("Topic '{Topic}' already exists (created concurrently)", result.Topic);
                else if (result.Error.IsError)
                    logger.LogError("Topic creation failed | name={Topic} | Reason: {Reason}",
                        result.Topic, result.Error.Reason);
            }
        }
    }

    /// <summary>
    /// Logs, per partition, the group's committed offset against the partition's high
    /// watermark. The difference is consumer lag: how many published events are still
    /// waiting to be processed.
    /// </summary>
    public async Task LogConsumerGroupLagAsync(IEnumerable<string> topics, CancellationToken ct = default)
    {
        using var admin = BuildAdminClient();
        var metadata = admin.GetMetadata(TimeSpan.FromSeconds(10));
        var wanted = topics.ToHashSet(StringComparer.Ordinal);

        var partitions = metadata.Topics
            .Where(t => wanted.Contains(t.Topic))
            .SelectMany(t => t.Partitions.Select(p => new TopicPartition(t.Topic, new Partition(p.PartitionId))))
            .ToList();

        if (partitions.Count == 0)
        {
            logger.LogWarning("No partitions found for topics [{Topics}]", string.Join(", ", wanted));
            return;
        }

        List<ListConsumerGroupOffsetsResult> committedResults;
        try
        {
            committedResults = await admin.ListConsumerGroupOffsetsAsync(
                [new ConsumerGroupTopicPartitions(_settings.GroupId, partitions)]);
        }
        catch (KafkaException ex)
        {
            logger.LogWarning("Could not read committed offsets for group '{Group}': {Reason}",
                _settings.GroupId, ex.Error.Reason);
            return;
        }

        // Watermarks are only exposed on a consumer handle, so borrow a short-lived one.
        using var probe = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = _settings.BootstrapServers,
            GroupId = _settings.GroupId + "-lag-probe",
            EnableAutoCommit = false
        })
        .SetLogHandler((_, _) => { })
        .Build();

        long totalLag = 0;

        foreach (var group in committedResults)
        foreach (var committed in group.Partitions)
        {
            ct.ThrowIfCancellationRequested();

            var watermarks = probe.QueryWatermarkOffsets(committed.TopicPartition, TimeSpan.FromSeconds(5));
            var highWatermark = watermarks.High.Value;

            // Offset.Unset means the group has never committed on this partition.
            var committedOffset = committed.Offset == Offset.Unset ? 0 : committed.Offset.Value;
            var lag = Math.Max(0, highWatermark - committedOffset);
            totalLag += lag;

            logger.LogInformation(
                "OFFSETS | group={Group} {Topic}[{Partition}] committed={Committed} highWatermark={High} lag={Lag}",
                _settings.GroupId, committed.Topic, committed.Partition.Value,
                committed.Offset == Offset.Unset ? "(none)" : committedOffset.ToString(),
                highWatermark, lag);
        }

        logger.LogInformation("OFFSETS | group={Group} totalLag={TotalLag}", _settings.GroupId, totalLag);
    }

    private IAdminClient BuildAdminClient() =>
        new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = _settings.BootstrapServers,
            SecurityProtocol = Enum.Parse<SecurityProtocol>(_settings.SecurityProtocol, ignoreCase: true)
        })
        .SetLogHandler((_, _) => { })   // admin client is chatty at startup; suppress
        .Build();
}
