namespace OrderFlow.Shared;

/// <summary>
/// One entry of the <c>Kafka:Topics</c> configuration array. Adding a topic here is all it
/// takes to have it provisioned at startup and, when <see cref="Subscribe"/> is true, read
/// by the consumer.
/// </summary>
public sealed class TopicSettings
{
    /// <summary>Topic name, e.g. "orders". Required.</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>
    /// Partition count for this topic. Leave unset to fall back to
    /// <see cref="KafkaSettings.TopicPartitions"/>. Raise it for a high-volume topic, since
    /// the partition count caps how many consumer instances can share its work.
    /// </summary>
    public int? Partitions { get; set; }

    /// <summary>
    /// Replication factor. Leave unset to fall back to
    /// <see cref="KafkaSettings.TopicReplicationFactor"/>. Must not exceed the number of
    /// brokers in the cluster.
    /// </summary>
    public short? ReplicationFactor { get; set; }

    /// <summary>
    /// Whether the consumer subscribes to this topic. Set false for a topic this service
    /// only writes to, or one provisioned for a different service to read.
    /// </summary>
    public bool Subscribe { get; set; } = true;

    public int ResolvePartitions(KafkaSettings defaults) => Partitions ?? defaults.TopicPartitions;

    public short ResolveReplicationFactor(KafkaSettings defaults) =>
        ReplicationFactor ?? defaults.TopicReplicationFactor;
}
