namespace OrderFlow.Producer.Models;

/// <summary>
/// Returned on a successful publish. The partition and offset come straight from the
/// broker's delivery report, which is what makes the write verifiable.
/// </summary>
public sealed record PublishResponse(
    Guid OrderId,
    Guid EventId,
    string CorrelationId,
    string EventType,
    string Topic,
    int Partition,
    long Offset,
    string Key);
