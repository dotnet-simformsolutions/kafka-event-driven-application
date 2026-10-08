namespace OrderFlow.Shared;

/// <summary>
/// Bound from the "Kafka" configuration section. In Docker every property can be
/// overridden with a double-underscore env var, e.g. <c>Kafka__BootstrapServers</c>.
/// </summary>
public sealed class KafkaSettings
{
    public const string SectionName = "Kafka";

    // ── Connection ──────────────────────────────────────────────────────────
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string SecurityProtocol { get; set; } = "PLAINTEXT";

    // ── Consumer group ──────────────────────────────────────────────────────
    public string GroupId { get; set; } = "order-processing-group";
    public int SessionTimeoutMs { get; set; } = 30_000;
    public int MaxPollIntervalMs { get; set; } = 300_000;

    /// <summary>
    /// Left false on purpose: offsets are committed manually after a message is
    /// successfully handled, which is what gives us at-least-once delivery.
    /// </summary>
    public bool EnableAutoCommit { get; set; } = false;

    /// <summary>"earliest" makes a brand-new consumer group replay the whole topic.</summary>
    public string AutoOffsetReset { get; set; } = "earliest";

    // ── Producer ────────────────────────────────────────────────────────────
    public int MessageTimeoutMs { get; set; } = 5_000;

    // ── Topic provisioning ──────────────────────────────────────────────────
    /// <summary>Default partition count, used by any topic that does not set its own.</summary>
    public int TopicPartitions { get; set; } = 3;

    /// <summary>Default replication factor, used by any topic that does not set its own.</summary>
    public short TopicReplicationFactor { get; set; } = 1;

    /// <summary>
    /// The topics this system uses, bound from the <c>Kafka:Topics</c> array. Every entry is
    /// created at startup if it does not exist; entries with <c>Subscribe: true</c> are also
    /// read by the consumer.
    /// </summary>
    public List<TopicSettings> Topics { get; set; } = [];

    /// <summary>Names of every configured topic, subscribed or not.</summary>
    public IEnumerable<string> AllTopicNames => Topics.Select(t => t.Name);

    /// <summary>Names of the topics the consumer should subscribe to.</summary>
    public IEnumerable<string> SubscribedTopicNames =>
        Topics.Where(t => t.Subscribe).Select(t => t.Name);

    // ── Retry ───────────────────────────────────────────────────────────────
    /// <summary>How many times a failing handler is retried in place before the message is logged and skipped.</summary>
    public int MaxRetryAttempts { get; set; } = 3;

    /// <summary>Base delay for exponential backoff: attempt N waits BaseDelay * 2^(N-1).</summary>
    public int RetryBaseDelayMs { get; set; } = 1_000;

    /// <summary>Ceiling on a single backoff wait, so a retry can never outlast MaxPollIntervalMs.</summary>
    public int RetryMaxDelayMs { get; set; } = 10_000;
}
