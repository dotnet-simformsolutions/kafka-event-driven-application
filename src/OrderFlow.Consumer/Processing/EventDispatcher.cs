using System.Collections.Frozen;
using System.Collections.Immutable;
using System.Text.Json;
using OrderFlow.Consumer.Handlers;
using OrderFlow.Consumer.Resilience;
using OrderFlow.Contracts;
using OrderFlow.Shared;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace OrderFlow.Consumer.Processing;

/// <summary>
/// Maps (topic, event-type) to a handler, deserializes the payload, and invokes it.
///
/// A fresh DI scope is created per dispatch, which is also per retry attempt. That is
/// what lets handlers take scoped dependencies (a DbContext, a unit of work) safely even
/// though the consumer itself is a singleton background service.
/// </summary>
public sealed class EventDispatcher(
    IServiceScopeFactory scopeFactory,
    ILogger<EventDispatcher> logger)
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    /// <summary>
    /// The single routing table: (topic, event-type) to the handler invocation.
    ///
    /// This replaced a <c>switch</c> plus a hand-maintained topic array. The array claimed
    /// proximity kept the two in sync, but nothing enforced it — adding a route and
    /// forgetting the array would let a topic escape the subscription check that exists
    /// precisely to stop a silent workflow stall. Deriving
    /// <see cref="RoutedTopics"/> from this dictionary makes that drift impossible rather
    /// than merely discouraged.
    /// </summary>
    private static readonly FrozenDictionary<(string Topic, string EventType),
        Func<IServiceProvider, string, CancellationToken, Task>> Routes =
        new Dictionary<(string, string), Func<IServiceProvider, string, CancellationToken, Task>>
        {
            [(KafkaTopics.Orders, OrderCreatedEvent.Type)] =
                (sp, payload, ct) => InvokeAsync<OrderCreatedEvent>(sp, payload, ct),

            [(KafkaTopics.Shipments, OrderShippedEvent.Type)] =
                (sp, payload, ct) => InvokeAsync<OrderShippedEvent>(sp, payload, ct),

            [(KafkaTopics.Deliveries, OrderDeliveredEvent.Type)] =
                (sp, payload, ct) => InvokeAsync<OrderDeliveredEvent>(sp, payload, ct),

            [(KafkaTopics.Notifications, NotificationRequestedEvent.Type)] =
                (sp, payload, ct) => InvokeAsync<NotificationRequestedEvent>(sp, payload, ct)
        }.ToFrozenDictionary();

    /// <summary>
    /// Every topic this dispatcher routes, derived from <see cref="Routes"/> so it cannot
    /// disagree with the actual routing. <c>ConsumerTopicValidator</c> uses it to fail
    /// startup when one of these topics is not subscribed — otherwise the topic would be
    /// created, published to, and never read, stalling the workflow with nothing logged.
    /// </summary>
    public static readonly ImmutableArray<string> RoutedTopics =
        [.. Routes.Keys.Select(k => k.Topic).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal)];

    public async Task DispatchAsync(string topic, string? eventType, string payload, CancellationToken cancellationToken)
    {
        if (!Routes.TryGetValue((topic, eventType ?? string.Empty), out var invoke))
        {
            // No handler is registered for this combination, and no amount of retrying
            // will create one, so this is permanent by definition.
            logger.LogError("UNROUTABLE | topic={Topic} eventType={EventType}", topic, eventType ?? "(missing)");
            throw new PermanentEventException(
                $"No handler registered for topic '{topic}' with event type '{eventType ?? "(missing)"}'.");
        }

        // A fresh scope per dispatch, which is also per retry attempt.
        using var scope = scopeFactory.CreateScope();
        await invoke(scope.ServiceProvider, payload, cancellationToken);
    }

    private static async Task InvokeAsync<TEvent>(
        IServiceProvider services, string payload, CancellationToken cancellationToken)
        where TEvent : class, IEvent
    {
        TEvent @event;

        try
        {
            @event = JsonSerializer.Deserialize<TEvent>(payload, JsonOptions)
                     ?? throw new PermanentEventException($"Payload deserialized to null for {typeof(TEvent).Name}.");
        }
        catch (JsonException ex)
        {
            // Malformed payload: a classic poison message. Retrying re-parses the same
            // bytes and fails identically, so fail fast instead of burning the retry budget.
            throw new PermanentEventException($"Malformed JSON payload for {typeof(TEvent).Name}: {ex.Message}", ex);
        }

        var handler = services.GetRequiredService<IEventHandler<TEvent>>();
        await handler.HandleAsync(@event, cancellationToken);
    }
}
