using OrderFlow.Contracts;
using Microsoft.Extensions.Logging;

namespace OrderFlow.Consumer.Handlers;

/// <summary>
/// Terminal handler for the notifications topic, which all three order stages feed.
/// It publishes nothing, so the event chain has a definite end.
/// </summary>
public sealed class NotificationRequestedHandler(
    ILogger<NotificationRequestedHandler> logger) : IEventHandler<NotificationRequestedEvent>
{
    public async Task HandleAsync(NotificationRequestedEvent @event, CancellationToken cancellationToken = default)
    {
        await Task.Delay(75, cancellationToken);

        logger.LogInformation(
            "==> [NOTIFICATION SERVICE] {Channel} sent | order={OrderId} correlationId={CorrelationId} to={Recipient} subject={Subject}",
            @event.Channel, @event.OrderId, @event.CorrelationId, @event.Recipient, @event.Subject);
    }
}
