using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using OrderFlow.Contracts;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace OrderFlow.Shared;

/// <summary>
/// Singleton wrapper over a single librdkafka producer handle. One handle per process
/// is the intended usage: it is thread-safe and maintains the internal send batches.
/// </summary>
public sealed class KafkaEventProducer : IEventProducer, IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly ILogger<KafkaEventProducer> _logger;

    public static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false
    };

    public KafkaEventProducer(IOptions<KafkaSettings> settings, ILogger<KafkaEventProducer> logger)
    {
        _logger = logger;
        var s = settings.Value;

        var config = new ProducerConfig
        {
            BootstrapServers = s.BootstrapServers,
            MessageTimeoutMs = s.MessageTimeoutMs,
            Acks = Acks.All,            // wait for all in-sync replicas before acking
            EnableIdempotence = true,   // no duplicates introduced by internal retries
            MaxInFlight = 5,            // max allowed while keeping idempotent ordering
            CompressionType = CompressionType.Snappy,
            LingerMs = 5,               // small batching window for throughput
            BatchSize = 65_536
        };

        _producer = new ProducerBuilder<string, string>(config)
            .SetErrorHandler((_, error) =>
                _logger.LogError("Kafka producer error | Code: {Code} | Reason: {Reason} | Fatal: {IsFatal}",
                    error.Code, error.Reason, error.IsFatal))
            .SetLogHandler((_, m) =>
                _logger.LogDebug("librdkafka | [{Facility}] {Message}", m.Facility, m.Message))
            .Build();
    }

    public async Task<DeliveryResult<string, string>> PublishAsync<TEvent>(
        string topic, TEvent @event, string? key = null, CancellationToken cancellationToken = default)
        where TEvent : IEvent
    {
        // Serialize against the concrete runtime type so subclass properties are included.
        var payload = JsonSerializer.Serialize(@event, @event.GetType(), JsonOptions);
        var messageKey = key ?? @event.EventId.ToString();

        var headers = new Headers
        {
            { EventHeaders.EventType,  Encoding.UTF8.GetBytes(@event.EventType) },
            { EventHeaders.EventId,    Encoding.UTF8.GetBytes(@event.EventId.ToString()) },
            { EventHeaders.OccurredOn, Encoding.UTF8.GetBytes(@event.OccurredOn.ToString("O")) }
        };

        if (@event is BaseEvent { CorrelationId.Length: > 0 } baseEvent)
            headers.Add(EventHeaders.CorrelationId, Encoding.UTF8.GetBytes(baseEvent.CorrelationId));

        try
        {
            var result = await _producer.ProduceAsync(
                topic,
                new Message<string, string> { Key = messageKey, Value = payload, Headers = headers },
                cancellationToken);

            _logger.LogInformation(
                "PUBLISHED | {EventType} | topic={Topic} partition={Partition} offset={Offset} key={Key}",
                @event.EventType, result.Topic, result.Partition.Value, result.Offset.Value, result.Message.Key);

            return result;
        }
        catch (ProduceException<string, string> ex)
        {
            _logger.LogError(ex,
                "PUBLISH FAILED | {EventType} | topic={Topic} key={Key} | Reason: {Reason}",
                @event.EventType, topic, messageKey, ex.Error.Reason);
            throw;
        }
    }

    public void Dispose()
    {
        // Block briefly so queued messages are not lost on shutdown.
        _producer.Flush(TimeSpan.FromSeconds(10));
        _producer.Dispose();
    }
}
