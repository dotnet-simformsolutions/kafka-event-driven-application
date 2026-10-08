using OrderFlow.Shared;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace OrderFlow.Consumer.Resilience;

/// <summary>Result of running an operation through <see cref="RetryPolicy"/>.</summary>
public sealed record RetryOutcome(bool Succeeded, int AttemptsMade, Exception? LastException)
{
    public static RetryOutcome Success(int attempts) => new(true, attempts, null);
    public static RetryOutcome Failure(int attempts, Exception ex) => new(false, attempts, ex);
}

/// <summary>
/// In-process retry with exponential backoff. Deliberately hand-rolled so the mechanics
/// are visible: attempt N waits RetryBaseDelayMs * 2^(N-1), capped at RetryMaxDelayMs.
///
/// Retrying happens before the offset is committed, so a message being retried is still
/// uncommitted and would be redelivered if the process died mid-retry. The total backoff
/// must stay well under MaxPollIntervalMs, or the broker evicts this consumer from the
/// group for failing to poll.
/// </summary>
public sealed class RetryPolicy(IOptions<KafkaSettings> settings, ILogger<RetryPolicy> logger)
{
    private readonly KafkaSettings _settings = settings.Value;

    public async Task<RetryOutcome> ExecuteAsync(
        Func<CancellationToken, Task> operation,
        string operationDescription,
        CancellationToken cancellationToken)
    {
        var maxAttempts = Math.Max(1, _settings.MaxRetryAttempts);
        Exception? lastException = null;

        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            try
            {
                await operation(cancellationToken);

                if (attempt > 1)
                {
                    logger.LogInformation(
                        "RETRY SUCCEEDED | {Operation} | recovered on attempt {Attempt}/{MaxAttempts}",
                        operationDescription, attempt, maxAttempts);
                }

                return RetryOutcome.Success(attempt);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Shutdown, not a processing failure. Leave the offset uncommitted so the
                // message is redelivered to whichever instance picks up the partition next.
                throw;
            }
            catch (PermanentEventException ex)
            {
                logger.LogError(ex,
                    "PERMANENT FAILURE | {Operation} | not retryable, abandoning message",
                    operationDescription);

                return RetryOutcome.Failure(attempt, ex);
            }
            catch (Exception ex)
            {
                lastException = ex;

                if (attempt == maxAttempts)
                {
                    logger.LogError(ex,
                        "RETRIES EXHAUSTED | {Operation} | {MaxAttempts} attempts failed, abandoning message",
                        operationDescription, maxAttempts);
                    break;
                }

                var delay = ComputeBackoff(attempt);

                logger.LogWarning(
                    "RETRY SCHEDULED | {Operation} | attempt {Attempt}/{MaxAttempts} failed ({ExceptionType}: {Message}) | retrying in {Delay}ms",
                    operationDescription, attempt, maxAttempts, ex.GetType().Name, ex.Message, delay.TotalMilliseconds);

                await Task.Delay(delay, cancellationToken);
            }
        }

        return RetryOutcome.Failure(maxAttempts, lastException!);
    }

    private TimeSpan ComputeBackoff(int attempt)
    {
        var exponential = _settings.RetryBaseDelayMs * Math.Pow(2, attempt - 1);
        var capped = Math.Min(exponential, _settings.RetryMaxDelayMs);
        return TimeSpan.FromMilliseconds(capped);
    }
}
