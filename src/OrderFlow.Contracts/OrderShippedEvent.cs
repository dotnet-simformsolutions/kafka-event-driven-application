using System.Text.Json.Serialization;

namespace OrderFlow.Contracts;

public sealed class OrderShippedEvent : BaseEvent
{
    public const string Type = "order.shipped";

    [JsonPropertyName("eventType")]
    public override string EventType => Type;

    [JsonPropertyName("orderId")]
    public Guid OrderId { get; init; }

    [JsonPropertyName("trackingNumber")]
    public string TrackingNumber { get; init; } = string.Empty;

    [JsonPropertyName("carrier")]
    public string Carrier { get; init; } = string.Empty;

    [JsonPropertyName("estimatedDelivery")]
    public DateTime EstimatedDelivery { get; init; }

    [JsonPropertyName("warehouseId")]
    public string WarehouseId { get; init; } = string.Empty;
}
