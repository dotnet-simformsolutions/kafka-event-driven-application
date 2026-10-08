using OrderFlow.Contracts;
using OrderFlow.Shared;
using Microsoft.Extensions.Logging;

namespace OrderFlow.Consumer.Handlers;

/// <summary>Terminal step of the order workflow: closes the order and awards loyalty points.</summary>
public sealed class OrderDeliveredHandler(
    IEventProducer producer,
    ILogger<OrderDeliveredHandler> logger) : IEventHandler<OrderDeliveredEvent>
{
    public async Task HandleAsync(OrderDeliveredEvent @event, CancellationToken cancellationToken = default)
    {
        logger.LogInformation(
            "==> [DELIVERY SERVICE] order={OrderId} correlationId={CorrelationId} deliveredAt={DeliveredAt:HH:mm:ss} signedBy={SignedBy}",
            @event.OrderId, @event.CorrelationId, @event.DeliveredAt, @event.SignedBy);

        if (@event.DeliveryNote is not null)
            logger.LogInformation("    [DELIVERY] note: {Note}", @event.DeliveryNote);

        await Task.Delay(100, cancellationToken);
        logger.LogInformation("    [ORDER SERVICE] order={OrderId} marked COMPLETED", @event.OrderId);

        await Task.Delay(50, cancellationToken);
        logger.LogInformation("    [LOYALTY] points awarded | order={OrderId}", @event.OrderId);

        var survey = new NotificationRequestedEvent
        {
            OrderId = @event.OrderId,
            CorrelationId = @event.CorrelationId,
            Channel = "Email",
            Recipient = "customer@example.com",
            Subject = "How did we do?",
            Body = "Your order was delivered. Tap to rate your experience."
        };

        await producer.PublishAsync(KafkaTopics.Notifications, survey, @event.OrderId.ToString(), cancellationToken);

        logger.LogInformation("*** LIFECYCLE COMPLETE | order={OrderId} correlationId={CorrelationId} ***",
            @event.OrderId, @event.CorrelationId);
    }
}
