using OrderFlow.Consumer.Processing;
using OrderFlow.Consumer.Handlers;
using OrderFlow.Consumer.Resilience;
using OrderFlow.Contracts;
using OrderFlow.Shared;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

var builder = Host.CreateApplicationBuilder(args);

builder.Logging.ClearProviders();
builder.Logging.AddSimpleConsole(o =>
{
    o.SingleLine = true;
    o.TimestampFormat = "HH:mm:ss.fff ";
});
builder.Logging.AddFilter("Confluent", LogLevel.Warning);

// Shared Kafka plumbing: settings, admin client, producer, topic provisioning.
builder.Services.AddOrderFlowInfrastructure(builder.Configuration);

// Failure handling: retry with backoff, then log and skip.
builder.Services.AddSingleton<RetryPolicy>();

// Consumer-only validation: every topic EventDispatcher routes must be subscribed.
// Lives here rather than in Shared because the producer never subscribes.
builder.Services.AddSingleton<IValidateOptions<KafkaSettings>, ConsumerTopicValidator>();

// Dispatch. Handlers are scoped and resolved from a fresh scope per message, so they are
// free to depend on scoped services even though the consumer is a singleton.
builder.Services.AddSingleton<EventDispatcher>();
builder.Services.AddScoped<IEventHandler<OrderCreatedEvent>, OrderCreatedHandler>();
builder.Services.AddScoped<IEventHandler<OrderShippedEvent>, OrderShippedHandler>();
builder.Services.AddScoped<IEventHandler<OrderDeliveredEvent>, OrderDeliveredHandler>();
builder.Services.AddScoped<IEventHandler<NotificationRequestedEvent>, NotificationRequestedHandler>();

// Registered after AddOrderFlowInfrastructure so KafkaTopicInitializer runs first and the
// topics exist before this subscribes.
builder.Services.AddHostedService<OrderEventConsumer>();

var host = builder.Build();

var logger = host.Services.GetRequiredService<ILogger<Program>>();
logger.LogInformation("Kafka EDA demo | consumer worker starting");

await host.RunAsync();
