using System.Collections.Immutable;

namespace OrderFlow.Shared;

/// <summary>
/// Topic names the code refers to directly.
///
/// These stay as <c>const</c> because they are used in the consumer's routing switch, and a
/// C# <c>case</c> pattern requires a compile-time constant. Which topics actually get
/// created, and which the consumer subscribes to, is driven by the <c>Kafka:Topics</c>
/// configuration array — not by this class.
///
/// Adding a topic to configuration alone is enough to have it provisioned and consumed.
/// You only need a constant here when code must name the topic, which in practice means
/// publishing to it or routing its events to a handler.
/// </summary>
public static class KafkaTopics
{
    public const string Orders        = "orders";
    public const string Shipments     = "shipments";
    public const string Deliveries    = "deliveries";
    public const string Notifications = "notifications";

    /// <summary>
    /// Topics that must exist in configuration because code publishes to them. Startup
    /// validation fails if any is missing — otherwise the first publish would fail at
    /// runtime, since broker-side topic auto-creation is deliberately disabled.
    ///
    /// This is a separate concern from the consumer's <c>EventDispatcher.RoutedTopics</c>,
    /// which lists topics that must additionally be <em>subscribed</em>. The two happen to
    /// hold the same four names today, because every topic in this workflow is both written
    /// and read. They are kept apart deliberately: publishing is a whole-system requirement
    /// checked in both services, while subscribing is meaningful only in the consumer, and
    /// a future write-only topic would appear here but not there.
    /// </summary>
    public static readonly ImmutableArray<string> RequiredByCode =
        [Orders, Shipments, Deliveries, Notifications];
}
