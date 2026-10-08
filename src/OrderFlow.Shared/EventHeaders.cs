namespace OrderFlow.Shared;

/// <summary>Kafka message header names used across the system.</summary>
public static class EventHeaders
{
    public const string EventType     = "event-type";
    public const string EventId       = "event-id";
    public const string OccurredOn    = "occurred-on";
    public const string CorrelationId = "correlation-id";
}
