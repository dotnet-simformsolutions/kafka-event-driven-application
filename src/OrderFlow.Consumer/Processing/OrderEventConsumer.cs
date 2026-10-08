using System.Text;
using Confluent.Kafka;
using OrderFlow.Consumer.Resilience;
using OrderFlow.Shared;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace OrderFlow.Consumer.Processing;

/// <summary>
/// Long-running background service that polls the order topics and processes each
/// message with retry-then-skip semantics.
///
/// Delivery guarantee is at-least-once: auto-commit is off and the offset is committed
/// only after a message has been handled, or after its retries are exhausted and it is
/// deliberately abandoned. If this process dies mid-handler, the offset was never
/// advanced, so the message is redelivered. Handlers therefore need to tolerate seeing
/// the same event twice.
///
/// A message that exhausts its retries is logged in full and skipped so the partition
/// keeps flowing. There is no dead-letter topic: the log line is the only surviving
/// record of that event, which is why it carries the whole payload.
/// </summary>
public sealed class OrderEventConsumer(
    IOptions<KafkaSettings> settings,
    EventDispatcher dispatcher,
    RetryPolicy retryPolicy,
    ILogger<OrderEventConsumer> logger) : BackgroundService
{
    private readonly KafkaSettings _settings = settings.Value;

    /// <summary>
    /// Identifies this instance in the logs. Inside Docker this is the container id, which
    /// makes it obvious which replica of the consumer group owns which partition.
    /// </summary>
    private static readonly string InstanceId =
        Environment.GetEnvironmentVariable("CONSUMER_INSTANCE_ID") ?? Environment.MachineName;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _settings.BootstrapServers,
            GroupId = _settings.GroupId,
            ClientId = $"consumer-{InstanceId}",

            // Manual commits are what make at-least-once processing possible.
            EnableAutoCommit = _settings.EnableAutoCommit,

            // A new group starts from the beginning of the topic, so events published while
            // no consumer existed are still processed once one joins.
            AutoOffsetReset = Enum.Parse<AutoOffsetReset>(_settings.AutoOffsetReset, ignoreCase: true),

            SessionTimeoutMs = _settings.SessionTimeoutMs,
            MaxPollIntervalMs = _settings.MaxPollIntervalMs,

            // Incremental rebalancing: adding or removing an instance moves only the
            // partitions that must move instead of stopping the whole group.
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky
        };

        using var consumer = new ConsumerBuilder<string, string>(config)
            .SetErrorHandler((_, error) =>
                logger.LogError("CONSUMER ERROR | instance={Instance} code={Code} fatal={IsFatal} reason={Reason}",
                    InstanceId, error.Code, error.IsFatal, error.Reason))
            // With CooperativeSticky these handlers receive only the incremental delta, not
            // the full set, so log the resulting total too or a no-change rebalance looks
            // like the instance lost everything.
            .SetPartitionsAssignedHandler((c, added) =>
            {
                var owned = c.Assignment.Union(added).Distinct().ToList();
                logger.LogInformation(
                    "PARTITIONS ASSIGNED | instance={Instance} group={Group} gained=[{Gained}] nowOwns={Count} owned=[{Owned}]",
                    InstanceId, _settings.GroupId, Describe(added), owned.Count, Describe(owned));
            })
            .SetPartitionsRevokedHandler((c, revoked) =>
            {
                var revokedPartitions = revoked.Select(p => p.TopicPartition).ToList();
                var remaining = c.Assignment.Except(revokedPartitions).ToList();
                logger.LogInformation(
                    "PARTITIONS REVOKED | instance={Instance} group={Group} gaveUp=[{GaveUp}] nowOwns={Count} owned=[{Owned}]",
                    InstanceId, _settings.GroupId, Describe(revokedPartitions), remaining.Count, Describe(remaining));
            })
            .SetPartitionsLostHandler((_, lost) =>
                logger.LogWarning("PARTITIONS LOST | instance={Instance} partitions=[{Partitions}]",
                    InstanceId, Describe(lost.Select(p => p.TopicPartition))))
            .SetLogHandler((_, _) => { })
            .Build();

        // Driven by configuration: every Kafka:Topics entry with Subscribe true.
        var subscribedTopics = _settings.SubscribedTopicNames.ToList();

        if (subscribedTopics.Count == 0)
        {
            logger.LogError(
                "CONSUMER IDLE | instance={Instance} | no Kafka:Topics entry has Subscribe set to true, " +
                "so there is nothing to read", InstanceId);
            return;
        }

        consumer.Subscribe(subscribedTopics);

        logger.LogInformation(
            "CONSUMER STARTED | instance={Instance} group={Group} topics=[{Topics}] autoCommit={AutoCommit} offsetReset={OffsetReset} maxRetries={MaxRetries}",
            InstanceId, _settings.GroupId, string.Join(", ", subscribedTopics),
            _settings.EnableAutoCommit, _settings.AutoOffsetReset, _settings.MaxRetryAttempts);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, string>? result = null;

                try
                {
                    // Short timeout keeps the loop responsive to shutdown while still polling.
                    result = consumer.Consume(TimeSpan.FromMilliseconds(500));
                    if (result?.Message is null) continue;

                    await ProcessMessageAsync(consumer, result, stoppingToken);
                }
                catch (ConsumeException ex) when (!stoppingToken.IsCancellationRequested)
                {
                    // Transport-level problem (broker unreachable, auth). Nothing to commit;
                    // back off briefly so we do not spin the CPU while the broker is down.
                    logger.LogError(ex, "CONSUME FAILED | instance={Instance} code={Code} reason={Reason}",
                        InstanceId, ex.Error.Code, ex.Error.Reason);
                    await Task.Delay(1_000, stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }
        finally
        {
            // Close() commits final offsets and leaves the group cleanly, which triggers an
            // immediate rebalance instead of making the group wait for the session timeout.
            consumer.Close();
            logger.LogInformation("CONSUMER CLOSED | instance={Instance}", InstanceId);
        }
    }

    private async Task ProcessMessageAsync(
        IConsumer<string, string> consumer,
        ConsumeResult<string, string> result,
        CancellationToken stoppingToken)
    {
        var eventType = ReadHeader(result.Message.Headers, EventHeaders.EventType);
        var correlationId = ReadHeader(result.Message.Headers, EventHeaders.CorrelationId);
        var descriptor = $"{result.Topic}[{result.Partition.Value}]@{result.Offset.Value}";

        logger.LogInformation(
            "CONSUMED | instance={Instance} {Descriptor} eventType={EventType} key={Key} correlationId={CorrelationId}",
            InstanceId, descriptor, eventType ?? "(missing)", result.Message.Key, correlationId ?? "(none)");

        var outcome = await retryPolicy.ExecuteAsync(
            ct => dispatcher.DispatchAsync(result.Topic, eventType, result.Message.Value, ct),
            descriptor,
            stoppingToken);

        if (outcome.Succeeded)
        {
            Commit(consumer, result, descriptor);
            return;
        }

        // Retries are spent. Log everything we know about the message and move on, so one
        // unprocessable event cannot block the rest of its partition. The payload is
        // included deliberately: with no dead-letter topic, this log line is the only
        // surviving record and the only way to replay the event by hand.
        logger.LogError(outcome.LastException,
            "PROCESSING ABANDONED | instance={Instance} {Descriptor} eventType={EventType} key={Key} " +
            "correlationId={CorrelationId} attempts={Attempts} | message skipped, payload: {Payload}",
            InstanceId, descriptor, eventType ?? "(missing)", result.Message.Key,
            correlationId ?? "(none)", outcome.AttemptsMade, result.Message.Value);

        Commit(consumer, result, descriptor);
    }

    private void Commit(IConsumer<string, string> consumer, ConsumeResult<string, string> result, string descriptor)
    {
        try
        {
            consumer.Commit(result);
            logger.LogInformation("COMMITTED | instance={Instance} {Descriptor} nextOffset={NextOffset}",
                InstanceId, descriptor, result.Offset.Value + 1);
        }
        catch (KafkaException ex)
        {
            // A failed commit means this message may be redelivered after a rebalance.
            // Safe here because processing is idempotent-tolerant by design.
            logger.LogError(ex, "COMMIT FAILED | {Descriptor} | reason={Reason}", descriptor, ex.Error.Reason);
        }
    }

    private static string Describe(IEnumerable<TopicPartition> partitions) =>
        string.Join(", ", partitions
            .OrderBy(p => p.Topic, StringComparer.Ordinal)
            .ThenBy(p => p.Partition.Value)
            .Select(p => $"{p.Topic}#{p.Partition.Value}"));

    private static string? ReadHeader(Headers headers, string key) =>
        headers.TryGetLastBytes(key, out var bytes) ? Encoding.UTF8.GetString(bytes) : null;
}
