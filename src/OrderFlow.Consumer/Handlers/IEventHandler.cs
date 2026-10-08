using OrderFlow.Contracts;

namespace OrderFlow.Consumer.Handlers;

/// <summary>
/// A handler owns the business reaction to one event type. Throwing from
/// <see cref="HandleAsync"/> is the signal that processing failed: the consumer
/// retries the message and eventually logs the failure and moves on.
/// </summary>
public interface IEventHandler<in TEvent> where TEvent : IEvent
{
    Task HandleAsync(TEvent @event, CancellationToken cancellationToken = default);
}
