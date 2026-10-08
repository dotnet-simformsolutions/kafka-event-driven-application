using System.Collections.Concurrent;

namespace OrderFlow.Consumer.Handlers;

/// <summary>
/// Lets the application produce failures on demand so the retry and skip paths can be
/// demonstrated end to end. Driven entirely by the customer id on the incoming order,
/// which keeps the simulation out of the event schema.
/// </summary>
public static class DemoFailureSimulator
{
    /// <summary>Always throws, so retries are exhausted and the message is logged and skipped.</summary>
    public const string AlwaysFailsCustomerId = "CUST-FAIL";

    /// <summary>Throws on the first two attempts then succeeds, so retry visibly recovers.</summary>
    public const string FlakyCustomerId = "CUST-FLAKY";

    private const int FlakyFailuresBeforeSuccess = 2;

    private static readonly ConcurrentDictionary<Guid, int> FlakyAttempts = new();

    /// <summary>
    /// Throws if the customer id is one of the demo triggers. Called at the top of a
    /// handler, before any real work happens.
    /// </summary>
    public static void ThrowIfSimulatedFailure(string customerId, Guid orderId)
    {
        if (string.Equals(customerId, AlwaysFailsCustomerId, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"Simulated permanent failure for order {orderId}: the payment gateway rejects account {customerId}.");
        }

        if (string.Equals(customerId, FlakyCustomerId, StringComparison.OrdinalIgnoreCase))
        {
            var attempt = FlakyAttempts.AddOrUpdate(orderId, 1, (_, previous) => previous + 1);

            if (attempt <= FlakyFailuresBeforeSuccess)
            {
                throw new TimeoutException(
                    $"Simulated transient failure {attempt} of {FlakyFailuresBeforeSuccess} for order {orderId}: inventory service timed out.");
            }

            FlakyAttempts.TryRemove(orderId, out _);
        }
    }
}
