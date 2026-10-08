namespace OrderFlow.Contracts;

/// <summary>
/// The shared event contract. Every message that crosses a Kafka topic in this
/// system implements this interface, so producers and consumers agree on the
/// envelope (identity, timing, discriminator) independently of the payload.
/// </summary>
public interface IEvent
{
    /// <summary>Unique id for this event instance. Use it for idempotency checks downstream.</summary>
    Guid EventId { get; }

    /// <summary>UTC timestamp of when the business fact occurred (not when it was published).</summary>
    DateTime OccurredOn { get; }

    /// <summary>
    /// Stable discriminator, e.g. "order.created". Travels in the Kafka
    /// <c>event-type</c> header so a consumer can route without deserializing first.
    /// </summary>
    string EventType { get; }
}
