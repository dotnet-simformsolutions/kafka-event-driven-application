using OrderFlow.Contracts;
using OrderFlow.Shared;
using Microsoft.Extensions.Logging;

namespace OrderFlow.Consumer.Handlers;

/// <summary>Confirms carrier pickup and advances the order to delivered.</summary>
public sealed class OrderShippedHandler(
    IEventProducer producer,
    ILogger<OrderShippedHandler> logger) : IEventHandler<OrderShippedEvent>
{
    public async Task HandleAsync(OrderShippedEvent @event, CancellationToken cancellationToken = default)
    {
        logger.LogInformation(
            "==> [SHIPPING SERVICE] order={OrderId} correlationId={CorrelationId} carrier={Carrier} tracking={Tracking} eta={ETA:yyyy-MM-dd}",
            @event.OrderId, @event.CorrelationId, @event.Carrier, @event.TrackingNumber, @event.EstimatedDelivery);

        await Task.Delay(400, cancellationToken);
        logger.LogInformation("    [CARRIER] picked up by {Carrier} from {Warehouse} | order={OrderId}",
            @event.Carrier, @event.WarehouseId, @event.OrderId);

        var delivered = new OrderDeliveredEvent
        {
            OrderId = @event.OrderId,
            CorrelationId = @event.CorrelationId,
            DeliveredAt = DateTime.UtcNow,
            SignedBy = "J. Doe",
            DeliveryNote = $"Delivered via {@event.Carrier}, tracking {@event.TrackingNumber}"
        };

        await producer.PublishAsync(KafkaTopics.Deliveries, delivered, @event.OrderId.ToString(), cancellationToken);

        var shippingAlert = new NotificationRequestedEvent
        {
            OrderId = @event.OrderId,
            CorrelationId = @event.CorrelationId,
            Channel = "SMS",
            Recipient = "+1-555-0100",
            Subject = "Your order has shipped",
            Body = $"Tracking {@event.TrackingNumber} with {@event.Carrier}, arriving {@event.EstimatedDelivery:yyyy-MM-dd}."
        };

        await producer.PublishAsync(KafkaTopics.Notifications, shippingAlert, @event.OrderId.ToString(), cancellationToken);
    }
}
