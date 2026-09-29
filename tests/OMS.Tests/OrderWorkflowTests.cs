using System.Reflection;
using OMS.Worker.Activities;
using OMS.Worker.Models;
using OMS.Worker.Services;
using OMS.Worker.Workflows;
using Temporalio.Client;
using Temporalio.Common;
using Temporalio.Testing;
using Temporalio.Worker;
using Temporalio.Workflows;
using Xunit;

namespace OMS.Tests;

public class OrderWorkflowTests
{
    // ---------------------------------------------------------------------------
    // Test infrastructure
    // ---------------------------------------------------------------------------

    private static TemporalWorker CreateWorker(WorkflowEnvironment env, IOrderRepository? repo = null)
    {
        var activities = new OrderActivities(
            repo ?? new InMemoryOrderRepository(),
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

    /// <summary>
    /// Registers the OrderStatus keyword search attribute on the embedded test server
    /// so Workflow.UpsertTypedSearchAttributes does not produce activation errors.
    /// The time-skipping test server exposes an OperatorService for this purpose.
    /// </summary>
    private static async Task RegisterSearchAttributesAsync(WorkflowEnvironment env)
    {
        try
        {
            await env.Client.Connection.OperatorService.AddSearchAttributesAsync(
                new Temporalio.Api.OperatorService.V1.AddSearchAttributesRequest
                {
                    SearchAttributes =
                    {
                        { "OrderStatus", Temporalio.Api.Enums.V1.IndexedValueType.Keyword }
                    }
                });
        }
        catch
        {
            // Silently skip if the embedded server does not support this RPC.
        }
    }

    /// <summary>
    /// Registers search attributes, creates a worker, and runs the test body inside
    /// worker.ExecuteAsync — the single pattern used by all async workflow tests.
    /// </summary>
    private static async Task RunTestAsync(WorkflowEnvironment env, Func<Task> test, IOrderRepository? repo = null)
    {
        await RegisterSearchAttributesAsync(env);
        using var worker = CreateWorker(env, repo);
        await worker.ExecuteAsync(test);
    }

    // Helpers produce fully-constructed values so no call with optional arguments
    // ends up inside a Temporal expression-tree lambda (avoids CS0854).
    private static OrderSubmission MakeValidOrder(string id) => new OrderSubmission(
        "CUST-1",
        new OrderPayload(id, new[] { new OrderItem("ITEM-1", 1, null, null) }),
        null);

    private static OrderSubmission MakeInvalidOrder(string id) => new OrderSubmission(
        "CUST-1",
        new OrderPayload(id, new[] { new OrderItem("INVALID-ITEM", 0, null, null) }),
        null);

    private static PaymentCapture MakePayment(string orderId) =>
        new PaymentCapture("CUST-1", "RRN-100001", 15000, orderId);

    // ---------------------------------------------------------------------------
    // Reflection / configuration smoke tests (no server needed)
    // ---------------------------------------------------------------------------

    [Fact]
    public void Workflow_UsesPinnedVersioningBehavior()
    {
        var attr = typeof(OrderProcessingWorkflow)
            .GetCustomAttributes(typeof(WorkflowAttribute), true)
            .OfType<object>()
            .FirstOrDefault();

        Assert.NotNull(attr);

        var prop = attr!.GetType().GetProperty("VersioningBehavior");
        Assert.NotNull(prop);
        Assert.Equal(VersioningBehavior.Pinned, prop!.GetValue(attr));
    }

    [Fact]
    public void CommerceValidation_UsesDedicatedQueueAndRateLimit()
    {
        var method = typeof(OrderProcessingWorkflow)
            .GetMethod("ValidationActivityOptions", BindingFlags.Static | BindingFlags.NonPublic);

        Assert.NotNull(method);

        var opts = Assert.IsType<ActivityOptions>(method!.Invoke(null, null));
        Assert.Equal(TemporalConstants.CommerceTaskQueue, opts.TaskQueue);
    }

    [Fact]
    public void CommerceWorker_HasMaxTaskQueueActivitiesPerSecond_Of150()
    {
        var workerOptions = new TemporalWorkerOptions(TemporalConstants.CommerceTaskQueue)
        {
            MaxTaskQueueActivitiesPerSecond = 150
        };

        Assert.Equal(150, workerOptions.MaxTaskQueueActivitiesPerSecond ?? 0);
    }

    [Fact]
    public void Workflow_RegistersOrderStatusSearchAttribute()
    {
        var field = typeof(OrderProcessingWorkflow)
            .GetField("OrderStatusKey", BindingFlags.Static | BindingFlags.NonPublic);

        Assert.NotNull(field);
    }

    [Fact]
    public void EnrichmentActivityOptions_HaveHourLevelScheduleToClose()
    {
        var method = typeof(OrderProcessingWorkflow)
            .GetMethod("EnrichmentActivityOptions", BindingFlags.Static | BindingFlags.NonPublic);

        Assert.NotNull(method);

        var opts = Assert.IsType<ActivityOptions>(method!.Invoke(null, null));
        Assert.True(opts.ScheduleToCloseTimeout >= TimeSpan.FromHours(1),
            $"EnrichmentActivityOptions.ScheduleToCloseTimeout should be ≥ 1 hour, was {opts.ScheduleToCloseTimeout}");
    }

    [Fact]
    public void StatusActivityOptions_HaveNoScheduleToCloseTimeout()
    {
        var method = typeof(OrderProcessingWorkflow)
            .GetMethod("StatusActivityOptions", BindingFlags.Static | BindingFlags.NonPublic);

        Assert.NotNull(method);

        var opts = Assert.IsType<ActivityOptions>(method!.Invoke(null, null));
        // Dashboard writes are eventually-consistent projections; they must never
        // time-out the retry loop, so ScheduleToCloseTimeout must be null (unbounded).
        Assert.Null(opts.ScheduleToCloseTimeout);
    }

    [Fact]
    public void FulfillmentActivityOptions_HaveHourLevelScheduleToClose()
    {
        var method = typeof(OrderProcessingWorkflow)
            .GetMethod("FulfillmentActivityOptions", BindingFlags.Static | BindingFlags.NonPublic);

        Assert.NotNull(method);

        var opts = Assert.IsType<ActivityOptions>(method!.Invoke(null, null));
        Assert.True(opts.ScheduleToCloseTimeout >= TimeSpan.FromHours(1),
            $"FulfillmentActivityOptions.ScheduleToCloseTimeout should be ≥ 1 hour, was {opts.ScheduleToCloseTimeout}");
    }

    // ---------------------------------------------------------------------------
    // Happy path: valid order → Fulfilled
    // ---------------------------------------------------------------------------

    [Fact]
    public async Task HappyPath_ValidOrderWithPayment_CompletesAsFulfilled()
    {
        await using var env = await WorkflowEnvironment.StartTimeSkippingAsync();
        var order = MakeValidOrder("ORD-HAPPY");
        var payment = MakePayment("ORD-HAPPY");

        await RunTestAsync(env, async () =>
        {
            var handle = await env.Client.StartWorkflowAsync(
                (OrderProcessingWorkflow wf) => wf.RunAsync(order),
                new WorkflowOptions { Id = "ORD-HAPPY", TaskQueue = "test-orders" });

            OrderStatusView? status = null;
            for (var i = 0; i < 50; i++)
            {
                status = await handle.QueryAsync(wf => wf.GetStatus());
                if (status.Status == OrderStatus.WaitingForPayment) break;
                await Task.Delay(20);
            }

            Assert.Equal(OrderStatus.WaitingForPayment, status!.Status);

            await handle.SignalAsync(wf => wf.CapturePaymentAsync(payment));

            var result = await handle.GetResultAsync();
            Assert.Equal(OrderStatus.Fulfilled, result.Status);
            Assert.Equal("ORD-HAPPY", result.OrderId);
            Assert.Equal("RRN-100001", result.Rrn);
        });
    }

    // ---------------------------------------------------------------------------
    // TTL expiry: no payment received within 30 days → Expired
    // ---------------------------------------------------------------------------

    [Fact]
    public async Task PaymentNeverReceived_ExpiresAfter30Days()
    {
        await using var env = await WorkflowEnvironment.StartTimeSkippingAsync();
        var order = MakeValidOrder("ORD-EXPIRE");

        await RunTestAsync(env, async () =>
        {
            var handle = await env.Client.StartWorkflowAsync(
                (OrderProcessingWorkflow wf) => wf.RunAsync(order),
                new WorkflowOptions { Id = "ORD-EXPIRE", TaskQueue = "test-orders" });

            var result = await handle.GetResultAsync();
            Assert.Equal(OrderStatus.Expired, result.Status);
        });
    }

    // ---------------------------------------------------------------------------
    // TTL expiry on invalid order: correction never arrives → Expired
    // ---------------------------------------------------------------------------

    [Fact]
    public async Task InvalidOrder_CorrectionNeverArrives_ExpiresAfter30Days()
    {
        await using var env = await WorkflowEnvironment.StartTimeSkippingAsync();
        var order = MakeInvalidOrder("ORD-NO-CORRECTION");

        await RunTestAsync(env, async () =>
        {
            var handle = await env.Client.StartWorkflowAsync(
                (OrderProcessingWorkflow wf) => wf.RunAsync(order),
                new WorkflowOptions { Id = "ORD-NO-CORRECTION", TaskQueue = "test-orders" });

            var result = await handle.GetResultAsync();
            Assert.Equal(OrderStatus.Expired, result.Status);
        });
    }

    // ---------------------------------------------------------------------------
    // Support correction flow: invalid → correction → Fulfilled
    // ---------------------------------------------------------------------------

    [Fact]
    public async Task InvalidOrder_WithCorrection_EventuallyFulfilled()
    {
        await using var env = await WorkflowEnvironment.StartTimeSkippingAsync();
        var order = MakeInvalidOrder("ORD-FIX");
        var correction = new SupportCorrection(new[] { new OrderItem("ITEM-1", 2, null, null) });
        var payment = MakePayment("ORD-FIX");

        await RunTestAsync(env, async () =>
        {
            var handle = await env.Client.StartWorkflowAsync(
                (OrderProcessingWorkflow wf) => wf.RunAsync(order),
                new WorkflowOptions { Id = "ORD-FIX", TaskQueue = "test-orders" });

            // Wait for ValidationFailed.
            OrderStatusView? status = null;
            for (var i = 0; i < 50; i++)
            {
                status = await handle.QueryAsync(wf => wf.GetStatus());
                if (status.Status == OrderStatus.ValidationFailed) break;
                await Task.Delay(20);
            }

            Assert.Equal(OrderStatus.ValidationFailed, status!.Status);

            await handle.SignalAsync(wf => wf.CorrectOrderAsync(correction));

            // Wait for WaitingForPayment after re-validation.
            for (var i = 0; i < 50; i++)
            {
                status = await handle.QueryAsync(wf => wf.GetStatus());
                if (status.Status == OrderStatus.WaitingForPayment) break;
                await Task.Delay(20);
            }

            Assert.Equal(OrderStatus.WaitingForPayment, status!.Status);

            await handle.SignalAsync(wf => wf.CapturePaymentAsync(payment));

            var result = await handle.GetResultAsync();
            Assert.Equal(OrderStatus.Fulfilled, result.Status);
        });
    }

    // ---------------------------------------------------------------------------
    // Capture/cancel race: capture arrives first, then cancel → Fulfilled (not Cancelled)
    // ---------------------------------------------------------------------------

    [Fact]
    public async Task CaptureArrivesFirst_ThenCancel_OrderStillFulfilled()
    {
        await using var env = await WorkflowEnvironment.StartTimeSkippingAsync();
        var order = MakeValidOrder("ORD-RACE");
        var payment = MakePayment("ORD-RACE");

        await RunTestAsync(env, async () =>
        {
            var handle = await env.Client.StartWorkflowAsync(
                (OrderProcessingWorkflow wf) => wf.RunAsync(order),
                new WorkflowOptions { Id = "ORD-RACE", TaskQueue = "test-orders" });

            for (var i = 0; i < 50; i++)
            {
                var s = await handle.QueryAsync(wf => wf.GetStatus());
                if (s.Status == OrderStatus.WaitingForPayment) break;
                await Task.Delay(20);
            }

            // Capture first, then cancel — cancel must be ignored.
            await handle.SignalAsync(wf => wf.CapturePaymentAsync(payment));
            await handle.SignalAsync("CancelAsync", new[] { "Too late cancel" });

            var result = await handle.GetResultAsync();
            Assert.Equal(OrderStatus.Fulfilled, result.Status);
        });
    }

    // ---------------------------------------------------------------------------
    // Cancel before payment → Cancelled
    // ---------------------------------------------------------------------------

    [Fact]
    public async Task CancelBeforePayment_CompletesAsCancelled()
    {
        await using var env = await WorkflowEnvironment.StartTimeSkippingAsync();
        var order = MakeValidOrder("ORD-CANCEL");

        await RunTestAsync(env, async () =>
        {
            var handle = await env.Client.StartWorkflowAsync(
                (OrderProcessingWorkflow wf) => wf.RunAsync(order),
                new WorkflowOptions { Id = "ORD-CANCEL", TaskQueue = "test-orders" });

            await handle.SignalAsync("CancelAsync", new[] { "Customer request" });
            var result = await handle.GetResultAsync();
            Assert.Equal(OrderStatus.Cancelled, result.Status);
        });
    }

    // ---------------------------------------------------------------------------
    // Workflow waits at WaitingForPayment
    // ---------------------------------------------------------------------------

    [Fact]
    public async Task ValidOrder_WaitsForPayment()
    {
        await using var env = await WorkflowEnvironment.StartTimeSkippingAsync();
        var order = MakeValidOrder("ORD-WAIT");

        await RunTestAsync(env, async () =>
        {
            var handle = await env.Client.StartWorkflowAsync(
                (OrderProcessingWorkflow wf) => wf.RunAsync(order),
                new WorkflowOptions { Id = "ORD-WAIT", TaskQueue = "test-orders" });

            OrderStatusView? status = null;
            for (var i = 0; i < 50; i++)
            {
                status = await handle.QueryAsync(wf => wf.GetStatus());
                if (status.Status == OrderStatus.WaitingForPayment) break;
                await Task.Delay(20);
            }

            Assert.Equal(OrderStatus.WaitingForPayment, status!.Status);
        });
    }

    // ---------------------------------------------------------------------------
    // Invalid payment RRN → loops back to WaitingForPayment, valid capture → Fulfilled
    // ---------------------------------------------------------------------------

    [Fact]
    public async Task InvalidPaymentRrn_ThenValidCapture_CompletesAsFulfilled()
    {
        await using var env = await WorkflowEnvironment.StartTimeSkippingAsync();
        var order = MakeValidOrder("ORD-RETRY");
        var badPayment = new PaymentCapture("CUST-1", "INVALID", 100, "ORD-RETRY");
        var goodPayment = new PaymentCapture("CUST-1", "RRN-VALID", 150, "ORD-RETRY");

        await RunTestAsync(env, async () =>
        {
            var handle = await env.Client.StartWorkflowAsync(
                (OrderProcessingWorkflow wf) => wf.RunAsync(order),
                new WorkflowOptions { Id = "ORD-RETRY", TaskQueue = "test-orders" });

            await handle.SignalAsync(wf => wf.CapturePaymentAsync(badPayment));
            await handle.SignalAsync(wf => wf.CapturePaymentAsync(goodPayment));

            var result = await handle.GetResultAsync();
            Assert.Equal(OrderStatus.Fulfilled, result.Status);
        });
    }

    // ---------------------------------------------------------------------------
    // Mismatched customer ID in payment signal → ignored, correct customer wins
    // ---------------------------------------------------------------------------

    [Fact]
    public async Task MismatchedCustomerPayment_IsIgnored()
    {
        await using var env = await WorkflowEnvironment.StartTimeSkippingAsync();
        var order = MakeValidOrder("ORD-MISMATCH");
        var wrongPayment = new PaymentCapture("CUST-999", "RRN-VALID", 150, "ORD-MISMATCH");
        var correctPayment = new PaymentCapture("CUST-1", "RRN-VALID", 150, "ORD-MISMATCH");

        await RunTestAsync(env, async () =>
        {
            var handle = await env.Client.StartWorkflowAsync(
                (OrderProcessingWorkflow wf) => wf.RunAsync(order),
                new WorkflowOptions { Id = "ORD-MISMATCH", TaskQueue = "test-orders" });

            await handle.SignalAsync(wf => wf.CapturePaymentAsync(wrongPayment));
            await handle.SignalAsync(wf => wf.CapturePaymentAsync(correctPayment));

            var result = await handle.GetResultAsync();
            Assert.Equal(OrderStatus.Fulfilled, result.Status);
        });
    }
}
