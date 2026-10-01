using OMS.Api;
using OMS.Worker.Models;
using OMS.Worker.Services;
using OpenTelemetry.Metrics;
using Temporalio.Client;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<IOrderRepository>(_ =>
    new SqliteOrderRepository(
        builder.Configuration.GetConnectionString("Orders") ?? "./data/orders.db"));
builder.Services.AddSingleton<OrderProcessingMetrics>();
builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics
        .AddAspNetCoreInstrumentation()
        .AddRuntimeInstrumentation()
        .AddMeter(OrderProcessingMetrics.MeterName)
        .AddPrometheusExporter());

// Use an async lazy so the DI container does not block a thread-pool thread during
// startup (avoids the sync-over-async anti-pattern of .GetAwaiter().GetResult()
// inside a DI factory). The client is created once on first use and shared.
builder.Services.AddSingleton<ITemporalClient>(sp =>
{
    var cfg = sp.GetRequiredService<IConfiguration>();
    return TemporalClient.ConnectAsync(new TemporalClientConnectOptions
    {
        TargetHost = cfg["Temporal:TargetHost"] ?? "localhost:7233",
        Namespace = cfg["Temporal:Namespace"] ?? TemporalConstants.Namespace
    }).GetAwaiter().GetResult();
});

builder.Services.AddHostedService<TemporalWorkerHostedService>();

builder.Services.AddControllers()
    .AddJsonOptions(opts =>
        opts.JsonSerializerOptions.Converters.Add(new System.Text.Json.Serialization.JsonStringEnumConverter()));

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();

app.MapControllers();
app.MapPrometheusScrapingEndpoint();

// Health endpoint: reports the configured Temporal endpoint and attempts a lightweight
// connectivity check (GetSystemInfoAsync). Returns degraded status on failure so that
// load-balancer health checks can distinguish "API running but Temporal unreachable"
// from "API not running".
app.MapGet("/health", async (ITemporalClient temporal, IConfiguration config) =>
{
    var endpoint = config["Temporal:TargetHost"] ?? "localhost:7233";
    try
    {
        // CheckHealthAsync is a cheap gRPC call that validates connectivity.
        await temporal.Connection.CheckHealthAsync();
        return Results.Ok(new { status = "ok", temporal = endpoint });
    }
    catch (Exception ex)
    {
        return Results.Json(
            new { status = "degraded", temporal = endpoint, error = ex.Message },
            statusCode: 503);
    }
});

app.Run();
