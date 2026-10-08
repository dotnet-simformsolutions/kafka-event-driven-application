namespace OrderFlow.Consumer.Resilience;

/// <summary>
/// Marks a failure that retrying cannot fix: malformed JSON, an unknown event type,
/// a message no handler is registered for. These skip the retry loop and are abandoned
/// immediately, because replaying them would only waste the consumer's time.
/// </summary>
public sealed class PermanentEventException(string message, Exception? innerException = null)
    : Exception(message, innerException);
