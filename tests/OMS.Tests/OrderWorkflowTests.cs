using System.Reflection;
using OMS.Worker.Activities;
using OMS.Worker.Models;
using OMS.Worker.Services;
using OMS.Worker.Workflows;
using Temporalio.Testing;
using Temporalio.Worker;
using Temporalio.Workflows;
using Xunit;

namespace OMS.Tests;

public class OrderWorkflowTests
{
    private static TemporalWorker CreateWorker(WorkflowEnvironment env)
    {
        var activities = new OrderActivities(
            new InMemoryOrderRepository(),
            new MockCommerceService(),
            new MockPimService(),
            new MockPaymentService(),
            new MockFulfillmentService(),
            new OrderProcessingMetrics());

        return new TemporalWorker(
            env.Client,
            new TemporalWorkerOptions("test-orders")
                .AddWorkflow<OrderProcessingWorkflow>()
                .AddActivity(activities.ValidateOrderAsync)
                .AddActivity(activities.EnrichOrderAsync)
                .AddActivity(activities.ValidatePaymentAsync)
                .AddActivity(activities.SaveStatusAsync)
                .AddActivity(activities.SaveFulfilledAsync)
                .AddActivity(activities.FulfillAsync)
                .AddActivity(activities.CompensateFulfillmentAsync));
    }

    private static OrderSubmission ValidOrder(string id = "ORD-TEST") => new(
        "CUST-1",
        new OrderPayload(id, new[]
        {
            new OrderItem("ITEM-1", 1)
        }));

    [Fact]
    public void ActivityOptions_AreBoundByScheduleToCloseNotMaxAttempts()
    {
        var method = typeof(OrderProcessingWorkflow)
            .GetMethod("ActivityOptions", BindingFlags.Static | BindingFlags.NonPublic);

        Assert.NotNull(method);

        var activityOptions = Assert.IsType<ActivityOptions>(method!.Invoke(null, null));
        Assert.Null(activityOptions.RetryPolicy);
    }

    [Fact]
    public async Task ValidOrder_WaitsForPayment()
    {
        await using var env = await WorkflowEnvironment.StartTimeSkippingAsync();

        var order = ValidOrder();

        using var worker = CreateWorker(env);

        await worker.ExecuteAsync(async () =>
        {
            var handle = await env.Client.StartWorkflowAsync(
                (OrderProcessingWorkflow wf) => wf.RunAsync(order),
                new(id: "ORD-TEST", taskQueue: "test-orders"));

            OrderStatusView? status = null;
            for (var attempt = 0; attempt < 50; attempt++)
            {
                status = await handle.QueryAsync(wf => wf.GetStatus());
                if (status.Status == OrderStatus.WaitingForPayment)
                {
                    break;
                }

                await Task.Delay(20);
            }

            Assert.NotNull(status);
            Assert.Equal(OrderStatus.WaitingForPayment, status!.Status);
        });
    }

    [Fact]
    public async Task CancelBeforePayment_CompletesAsCancelled()
    {
        await using var env = await WorkflowEnvironment.StartTimeSkippingAsync();
        using var worker = CreateWorker(env);

        await worker.ExecuteAsync(async () =>
        {
            var handle = await env.Client.StartWorkflowAsync(
                (OrderProcessingWorkflow wf) => wf.RunAsync(ValidOrder("ORD-CANCEL")),
                new(id: "ORD-CANCEL", taskQueue: "test-orders"));

            await handle.SignalAsync(wf => wf.CancelAsync("Customer request"));
            var result = await handle.GetResultAsync();

            Assert.Equal(OrderStatus.Cancelled, result.Status);
        });
    }

    [Fact]
    public async Task InvalidPayment_CompletesAsPaymentRejected()
    {
        await using var env = await WorkflowEnvironment.StartTimeSkippingAsync();
        using var worker = CreateWorker(env);

        await worker.ExecuteAsync(async () =>
        {
            var handle = await env.Client.StartWorkflowAsync(
                (OrderProcessingWorkflow wf) => wf.RunAsync(ValidOrder("ORD-PAYMENT")),
                new(id: "ORD-PAYMENT", taskQueue: "test-orders"));

            await handle.SignalAsync(wf => wf.CapturePaymentAsync(
                new PaymentCapture("CUST-1", "INVALID", 100, "ORD-PAYMENT")));
            var result = await handle.GetResultAsync();

            Assert.Equal(OrderStatus.PaymentRejected, result.Status);
        });
    }
}
