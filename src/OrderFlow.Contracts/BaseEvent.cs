using System.Text.Json.Serialization;

namespace OrderFlow.Contracts;

public abstract class BaseEvent : IEvent
{
    [JsonPropertyName("eventId")]
    public Guid EventId { get; init; } = Guid.NewGuid();

    [JsonPropertyName("occurredOn")]
    public DateTime OccurredOn { get; init; } = DateTime.UtcNow;

    [JsonPropertyName("eventType")]
    public abstract string EventType { get; }

    /// <summary>
    /// Correlation id shared by every event in one order's lifecycle, so the whole
    /// chain can be traced across services and topics.
    /// </summary>
    [JsonPropertyName("correlationId")]
    public string CorrelationId { get; init; } = string.Empty;
}
