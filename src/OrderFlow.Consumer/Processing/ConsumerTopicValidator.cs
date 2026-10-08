using Microsoft.Extensions.Options;
using OrderFlow.Shared;

namespace OrderFlow.Consumer.Processing;

/// <summary>
/// Consumer-side configuration validation, registered only by this service.
///
/// The shared <see cref="KafkaSettingsValidator"/> checks that required topics are present
/// in configuration. It deliberately does not check <see cref="TopicSettings.Subscribe"/>,
/// because the producer loads the same section and never subscribes to anything — enforcing
/// it there would fail a perfectly valid producer configuration.
///
/// So the subscription requirement lives here, next to the service that actually consumes:
/// every topic <see cref="EventDispatcher.RoutedTopics"/> routes must be subscribed. Without
/// this, setting <c>Subscribe: false</c> on, say, <c>shipments</c> passes every other check,
/// creates the topic, and lets handlers keep publishing to it — while nothing ever reads it.
/// The order lifecycle then stops after the first step with no error anywhere.
/// </summary>
public sealed class ConsumerTopicValidator : IValidateOptions<KafkaSettings>
{
    public ValidateOptionsResult Validate(string? name, KafkaSettings settings)
    {
        var errors = new List<string>();

        var subscribed = settings.SubscribedTopicNames.ToHashSet(StringComparer.Ordinal);
        var configured = settings.AllTopicNames.ToHashSet(StringComparer.Ordinal);

        foreach (var topic in EventDispatcher.RoutedTopics)
        {
            // Absence is already reported by KafkaSettingsValidator; only report the
            // present-but-not-subscribed case here, so one mistake yields one message.
            if (configured.Contains(topic) && !subscribed.Contains(topic))
            {
                errors.Add(
                    $"Kafka:Topics entry '{topic}' has Subscribe set to false, but this consumer routes " +
                    "events from it. Nothing would read the topic and the workflow would stall silently. " +
                    "Set Subscribe to true.");
            }
        }

        // Only reachable when the loop above found nothing — either because no route is
        // registered, or because every routed topic is missing from configuration (already
        // reported elsewhere). Guarded so that setting Subscribe:false on all four topics
        // yields four messages rather than five for the same single mistake.
        if (errors.Count == 0 && settings.Topics.Count > 0 && subscribed.Count == 0)
        {
            errors.Add(
                "No Kafka:Topics entry has Subscribe set to true, so the consumer would have nothing " +
                "to read.");
        }

        return errors.Count > 0
            ? ValidateOptionsResult.Fail(errors)
            : ValidateOptionsResult.Success;
    }
}
