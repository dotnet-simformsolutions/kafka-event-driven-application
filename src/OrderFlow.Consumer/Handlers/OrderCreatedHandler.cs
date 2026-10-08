using OrderFlow.Contracts;
using OrderFlow.Shared;
using Microsoft.Extensions.Logging;

namespace OrderFlow.Consumer.Handlers;

/// <summary>
/// Reserves inventory, takes payment, then fans out: an OrderShippedEvent to move the
/// workflow forward and a NotificationRequestedEvent to tell the customer.
/// </summary>
public sealed class OrderCreatedHandler(
    IEventProducer producer,
    ILogger<OrderCreatedHandler> logger) : IEventHandler<OrderCreatedEvent>
{
    public async Task HandleAsync(OrderCreatedEvent @event, CancellationToken cancellationToken = default)
    {
        logger.LogInformation(
            "==> [ORDER SERVICE] order={OrderId} correlationId={CorrelationId} customer={CustomerName} items={ItemCount} total={Currency} {Amount:F2}",
            @event.OrderId, @event.CorrelationId, @event.CustomerName,
            @event.Items.Count, @event.Currency, @event.TotalAmount);

        // Demo hook: raises an exception for the CUST-FAIL and CUST-FLAKY customer ids.
        DemoFailureSimulator.ThrowIfSimulatedFailure(@event.CustomerId, @event.OrderId);

        await Task.Delay(300, cancellationToken);
        logger.LogInformation("    [INVENTORY] reserved | order={OrderId}", @event.OrderId);

        await Task.Delay(200, cancellationToken);
        logger.LogInformation("    [PAYMENT] confirmed | order={OrderId} amount={Currency} {Amount:F2}",
            @event.OrderId, @event.Currency, @event.TotalAmount);

        var shipped = new OrderShippedEvent
        {
            OrderId = @event.OrderId,
            CorrelationId = @event.CorrelationId,
            TrackingNumber = $"TRK-{Guid.NewGuid().ToString()[..8].ToUpperInvariant()}",
            Carrier = "FedEx",
            EstimatedDelivery = DateTime.UtcNow.AddDays(3),
            WarehouseId = "WH-EAST-01"
        };

        await producer.PublishAsync(KafkaTopics.Shipments, shipped, @event.OrderId.ToString(), cancellationToken);

        var confirmation = new NotificationRequestedEvent
        {
            OrderId = @event.OrderId,
            CorrelationId = @event.CorrelationId,
            Channel = "Email",
            Recipient = @event.CustomerId + "@example.com",
            Subject = "Order confirmed",
            Body = $"Thanks {@event.CustomerName}, we received your order for {@event.Currency} {@event.TotalAmount:F2}."
        };

        await producer.PublishAsync(KafkaTopics.Notifications, confirmation, @event.OrderId.ToString(), cancellationToken);

        logger.LogInformation("    [ORDER SERVICE] shipment triggered | order={OrderId} tracking={Tracking}",
            @event.OrderId, shipped.TrackingNumber);
    }
}
