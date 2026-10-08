using System.Text.Json.Serialization;

namespace OrderFlow.Contracts;

/// <summary>
/// Fan-out event: every stage of the order lifecycle raises one of these, and a
/// single handler turns it into a customer-facing message. Demonstrates one topic
/// fed by multiple producers.
/// </summary>
public sealed class NotificationRequestedEvent : BaseEvent
{
    public const string Type = "notification.requested";

    [JsonPropertyName("eventType")]
    public override string EventType => Type;

    [JsonPropertyName("orderId")]
    public Guid OrderId { get; init; }

    [JsonPropertyName("channel")]
    public string Channel { get; init; } = "Email";

    [JsonPropertyName("recipient")]
    public string Recipient { get; init; } = string.Empty;

    [JsonPropertyName("subject")]
    public string Subject { get; init; } = string.Empty;

    [JsonPropertyName("body")]
    public string Body { get; init; } = string.Empty;
}
