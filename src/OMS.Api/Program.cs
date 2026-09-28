using Microsoft.Extensions.DependencyInjection;
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

builder.Services.AddSingleton<ITemporalClient>(sp =>
{
    return TemporalClient.ConnectAsync(new()
    {
        TargetHost = builder.Configuration["Temporal:TargetHost"] ?? "localhost:7233",
        Namespace = builder.Configuration["Temporal:Namespace"] ?? TemporalConstants.Namespace
    }).GetAwaiter().GetResult();
});

builder.Services.AddHostedService<TemporalWorkerHostedService>();

builder.Services.AddControllers();

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();

app.MapControllers();
app.MapPrometheusScrapingEndpoint();

app.MapGet("/health", (IConfiguration config) =>
    Results.Ok(new
    {
        status = "ok",
        temporal = config["Temporal:TargetHost"] ?? "localhost:7233"
    }));

app.Run();
