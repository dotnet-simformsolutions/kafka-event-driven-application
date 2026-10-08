using System.Text.Json.Serialization;

namespace OrderFlow.Contracts;

public sealed class OrderDeliveredEvent : BaseEvent
{
    public const string Type = "order.delivered";

    [JsonPropertyName("eventType")]
    public override string EventType => Type;

    [JsonPropertyName("orderId")]
    public Guid OrderId { get; init; }

    [JsonPropertyName("deliveredAt")]
    public DateTime DeliveredAt { get; init; }

    [JsonPropertyName("signedBy")]
    public string SignedBy { get; init; } = string.Empty;

    [JsonPropertyName("deliveryNote")]
    public string? DeliveryNote { get; init; }
}
