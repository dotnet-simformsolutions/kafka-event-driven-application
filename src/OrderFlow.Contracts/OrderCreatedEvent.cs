using System.Text.Json.Serialization;

namespace OrderFlow.Contracts;

public sealed class OrderCreatedEvent : BaseEvent
{
    public const string Type = "order.created";

    [JsonPropertyName("eventType")]
    public override string EventType => Type;

    [JsonPropertyName("orderId")]
    public Guid OrderId { get; init; }

    [JsonPropertyName("customerId")]
    public string CustomerId { get; init; } = string.Empty;

    [JsonPropertyName("customerName")]
    public string CustomerName { get; init; } = string.Empty;

    [JsonPropertyName("items")]
    public List<OrderItem> Items { get; init; } = [];

    [JsonPropertyName("totalAmount")]
    public decimal TotalAmount { get; init; }

    [JsonPropertyName("currency")]
    public string Currency { get; init; } = "USD";
}

public sealed class OrderItem
{
    [JsonPropertyName("productId")]
    public string ProductId { get; init; } = string.Empty;

    [JsonPropertyName("productName")]
    public string ProductName { get; init; } = string.Empty;

    [JsonPropertyName("quantity")]
    public int Quantity { get; init; }

    [JsonPropertyName("unitPrice")]
    public decimal UnitPrice { get; init; }
}
