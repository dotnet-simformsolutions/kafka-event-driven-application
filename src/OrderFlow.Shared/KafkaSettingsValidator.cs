using Microsoft.Extensions.Options;

namespace OrderFlow.Shared;

/// <summary>
/// Validates the <c>Kafka</c> configuration section at startup, so a bad topic list fails
/// immediately with a readable message instead of at the first publish.
/// </summary>
public sealed class KafkaSettingsValidator : IValidateOptions<KafkaSettings>
{
    /// <summary>
    /// How much of <c>MaxPollIntervalMs</c> retry backoff is allowed to consume. The
    /// remainder is headroom for handler execution and the rest of the fetched batch, which
    /// this validator cannot measure.
    /// </summary>
    private const double MaxPollIntervalBackoffShare = 0.8;


    public ValidateOptionsResult Validate(string? name, KafkaSettings settings)
    {
        var errors = new List<string>();

        if (string.IsNullOrWhiteSpace(settings.BootstrapServers))
            errors.Add("Kafka:BootstrapServers must be set.");

        if (string.IsNullOrWhiteSpace(settings.GroupId))
            errors.Add("Kafka:GroupId must be set.");

        // ── Fallback defaults ───────────────────────────────────────────────
        // Validated here because a topic that omits Partitions/ReplicationFactor — the
        // documented normal case — resolves to these. Left unchecked, a typo like
        // TopicPartitions: 0 passes startup and surfaces later as an opaque broker error
        // from CreateTopicsAsync.
        if (settings.TopicPartitions < 1)
            errors.Add($"Kafka:TopicPartitions must be at least 1 (was {settings.TopicPartitions}).");

        if (settings.TopicReplicationFactor < 1)
            errors.Add($"Kafka:TopicReplicationFactor must be at least 1 (was {settings.TopicReplicationFactor}).");

        // ── Topic list ──────────────────────────────────────────────────────
        if (settings.Topics.Count == 0)
        {
            errors.Add("Kafka:Topics must contain at least one topic.");
        }
        else
        {
            for (var i = 0; i < settings.Topics.Count; i++)
            {
                var topic = settings.Topics[i];

                if (string.IsNullOrWhiteSpace(topic.Name))
                    errors.Add($"Kafka:Topics[{i}]:Name must not be empty.");

                // A null here is legitimate — it means "use the Kafka:TopicPartitions
                // default", which is validated separately above. Only an explicit
                // non-positive value is an error.
                if (topic.Partitions is <= 0)
                    errors.Add($"Kafka:Topics[{i}]:Partitions must be greater than zero (topic '{topic.Name}').");

                if (topic.ReplicationFactor is <= 0)
                    errors.Add($"Kafka:Topics[{i}]:ReplicationFactor must be greater than zero (topic '{topic.Name}').");
            }

            var duplicates = settings.Topics
                .Where(t => !string.IsNullOrWhiteSpace(t.Name))
                .GroupBy(t => t.Name, StringComparer.Ordinal)
                .Where(g => g.Count() > 1)
                .Select(g => g.Key)
                .ToList();

            if (duplicates.Count > 0)
                errors.Add($"Kafka:Topics contains duplicate names: {string.Join(", ", duplicates)}.");

            // The safety net: broker-side auto-creation is off, so a topic the code
            // publishes to but configuration omits would fail on first use.
            var configured = settings.AllTopicNames.ToHashSet(StringComparer.Ordinal);
            var missing = KafkaTopics.RequiredByCode.Where(t => !configured.Contains(t)).ToList();

            if (missing.Count > 0)
            {
                errors.Add(
                    $"Kafka:Topics is missing topics the code requires: {string.Join(", ", missing)}. " +
                    "Handlers publish to these, and broker auto-creation is disabled, so they must be configured.");
            }
        }

        // ── Retry ───────────────────────────────────────────────────────────
        if (settings.MaxRetryAttempts < 1)
            errors.Add("Kafka:MaxRetryAttempts must be at least 1.");

        if (settings.RetryBaseDelayMs < 0)
            errors.Add("Kafka:RetryBaseDelayMs must not be negative.");

        if (settings.RetryMaxDelayMs < settings.RetryBaseDelayMs)
            errors.Add("Kafka:RetryMaxDelayMs must be greater than or equal to Kafka:RetryBaseDelayMs.");

        // Total backoff has to stay inside the poll interval, or the broker evicts this
        // consumer from the group mid-processing.
        //
        // RetryPolicy sleeps *between* attempts and breaks out before sleeping after the
        // final one, so N attempts produce N-1 waits, not N.
        //
        // Sleeping is only part of what MaxPollIntervalMs bounds: it also covers the handler
        // running on every attempt, plus the remaining messages in the fetched batch. None of
        // that is knowable here, so the budget is checked against a fraction of the interval
        // rather than the whole of it. Comparing against the full interval would approve a
        // config whose sleeps alone consume 99.9% of it, leaving no room to do any work.
        var backoffCount = Math.Max(0, settings.MaxRetryAttempts - 1);
        var worstCaseBackoffMs = (long)backoffCount * settings.RetryMaxDelayMs;
        var budgetMs = (long)(settings.MaxPollIntervalMs * MaxPollIntervalBackoffShare);

        if (worstCaseBackoffMs >= budgetMs)
        {
            errors.Add(
                $"Retry budget is too large: {backoffCount} wait(s) between " +
                $"{settings.MaxRetryAttempts} attempts x RetryMaxDelayMs " +
                $"({settings.RetryMaxDelayMs}ms) = {worstCaseBackoffMs}ms, which leaves too little of " +
                $"MaxPollIntervalMs ({settings.MaxPollIntervalMs}ms) for the handler itself. Backoff must " +
                $"stay under {MaxPollIntervalBackoffShare:P0} of the poll interval, i.e. below {budgetMs}ms. " +
                "Note that handler execution time is not measured here, so this is a floor, not a guarantee.");
        }

        return errors.Count > 0
            ? ValidateOptionsResult.Fail(errors)
            : ValidateOptionsResult.Success;
    }
}
