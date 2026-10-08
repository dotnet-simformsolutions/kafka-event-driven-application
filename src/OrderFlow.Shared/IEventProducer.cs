using Confluent.Kafka;
using OrderFlow.Contracts;

namespace OrderFlow.Shared;

public interface IEventProducer
{
    /// <summary>
    /// Publishes an event as JSON. <paramref name="key"/> decides the partition, so
    /// pass the order id to keep one order's events ordered on a single partition.
    /// </summary>
    Task<DeliveryResult<string, string>> PublishAsync<TEvent>(
        string topic,
        TEvent @event,
        string? key = null,
        CancellationToken cancellationToken = default)
        where TEvent : IEvent;
}
