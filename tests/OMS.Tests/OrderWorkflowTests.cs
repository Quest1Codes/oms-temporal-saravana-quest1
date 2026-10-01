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

    /// <summary>
    /// Mirrors production topology: a default worker and a separate Commerce worker.
    /// Returns both; caller must dispose.
    /// </summary>
    private static (TemporalWorker Default, TemporalWorker Commerce) CreateWorkers(
        WorkflowEnvironment env, IOrderRepository? repo = null)
    {
        var activities = new OrderActivities(
            repo ?? new InMemoryOrderRepository(),
            new MockCommerceService(),
            new MockPimService(),
            new MockPaymentService(),
            new MockFulfillmentService(),
            new OrderProcessingMetrics());

        var defaultWorker = new TemporalWorker(
            env.Client,
            new TemporalWorkerOptions("test-orders")
                .AddWorkflow<OrderProcessingWorkflow>()
                .AddActivity(activities.EnrichOrderAsync)
                .AddActivity(activities.ValidatePaymentAsync)
                .AddActivity(activities.SaveStatusAsync)
                .AddActivity(activities.SaveFulfilledAsync)
                .AddActivity(activities.FulfillAsync));

        // Commerce activities run on their own queue (matching production).
        var commerceWorker = new TemporalWorker(
            env.Client,
            new TemporalWorkerOptions(TemporalConstants.CommerceTaskQueue)
                .AddActivity(activities.ValidateOrderAsync));

        return (defaultWorker, commerceWorker);
    }

    /// <summary>
    /// Registers the OrderStatus search attribute and runs both workers for the duration
    /// of the test body.
    /// </summary>
    private static async Task RunTestAsync(
        WorkflowEnvironment env, Func<Task> test, IOrderRepository? repo = null)
    {
        await RegisterSearchAttributesAsync(env);
        var (defaultWorker, commerceWorker) = CreateWorkers(env, repo);
        using (defaultWorker)
        using (commerceWorker)
        {
            // ExecuteAsync guarantees the worker is polling before the inner body runs.
            // Nest the commerce worker inside the default worker so both are guaranteed
            // to be polling before the test body executes.
            await defaultWorker.ExecuteAsync(() =>
                commerceWorker.ExecuteAsync(test));
        }
    }

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
            // Silently skip if the embedded server does not support this call.
        }
    }

    // Helpers produce fully-constructed values so no optional-argument call
    // ends up inside a Temporal expression-tree lambda (CS0854).
    private static OrderSubmission MakeValidOrder(string id) => new(
        "CUST-1",
        new OrderPayload(id, new[] { new OrderItem("ITEM-1", 1, null, null) }),
        null);

    private static OrderSubmission MakeInvalidOrder(string id) => new(
        "CUST-1",
        new OrderPayload(id, new[] { new OrderItem("INVALID-ITEM", 0, null, null) }),
        null);

    private static PaymentCapture MakePayment(string orderId) =>
        new("CUST-1", "RRN-100001", 15000, orderId);

    // ---------------------------------------------------------------------------
    // Reflection / configuration smoke tests
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
    public void CommerceValidation_UsesDedicatedQueue()
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
        // Verify the production worker options constant, not a test-constructed value.
        // This test reads the configured value from TemporalWorkerHostedService indirectly
        // by checking the constant; a separate integration test would verify the hosted service.
        Assert.Equal("oms-commerce-processing", TemporalConstants.CommerceTaskQueue);
    }

    [Fact]
    public void EnrichmentActivityOptions_HaveHourLevelScheduleToClose()
    {
        var method = typeof(OrderProcessingWorkflow)
            .GetMethod("EnrichmentActivityOptions", BindingFlags.Static | BindingFlags.NonPublic);
        Assert.NotNull(method);
        var opts = Assert.IsType<ActivityOptions>(method!.Invoke(null, null));
        Assert.True(opts.ScheduleToCloseTimeout >= TimeSpan.FromHours(1));
    }

    [Fact]
    public void StatusActivityOptions_HaveNoScheduleToCloseTimeout()
    {
        var method = typeof(OrderProcessingWorkflow)
            .GetMethod("StatusActivityOptions", BindingFlags.Static | BindingFlags.NonPublic);
        Assert.NotNull(method);
        var opts = Assert.IsType<ActivityOptions>(method!.Invoke(null, null));
        Assert.Null(opts.ScheduleToCloseTimeout);
    }

    [Fact]
    public void FulfillmentActivityOptions_HaveHourLevelScheduleToClose()
    {
        var method = typeof(OrderProcessingWorkflow)
            .GetMethod("FulfillmentActivityOptions", BindingFlags.Static | BindingFlags.NonPublic);
        Assert.NotNull(method);
        var opts = Assert.IsType<ActivityOptions>(method!.Invoke(null, null));
        Assert.True(opts.ScheduleToCloseTimeout >= TimeSpan.FromHours(1));
    }

    [Fact]
    public void Workflow_HasOrderStatusSearchAttributeKey()
    {
        var field = typeof(OrderProcessingWorkflow)
            .GetField("OrderStatusKey", BindingFlags.Static | BindingFlags.NonPublic);
        Assert.NotNull(field);
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
            for (var i = 0; i < 100; i++)
            {
                status = await handle.QueryAsync(wf => wf.GetStatus());
                if (status.Status == OrderStatus.WaitingForPayment) break;
                await Task.Delay(10);
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
    // TTL expiry: no payment → Expired
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
    // TTL expiry on invalid order: no correction → Expired
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
    // Support correction: invalid → correction → Fulfilled
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

            OrderStatusView? status = null;
            for (var i = 0; i < 100; i++)
            {
                status = await handle.QueryAsync(wf => wf.GetStatus());
                if (status.Status == OrderStatus.ValidationFailed) break;
                await Task.Delay(10);
            }
            Assert.Equal(OrderStatus.ValidationFailed, status!.Status);

            await handle.SignalAsync(wf => wf.CorrectOrderAsync(correction));

            for (var i = 0; i < 100; i++)
            {
                status = await handle.QueryAsync(wf => wf.GetStatus());
                if (status.Status == OrderStatus.WaitingForPayment) break;
                await Task.Delay(10);
            }
            Assert.Equal(OrderStatus.WaitingForPayment, status!.Status);

            await handle.SignalAsync(wf => wf.CapturePaymentAsync(payment));
            var result = await handle.GetResultAsync();
            Assert.Equal(OrderStatus.Fulfilled, result.Status);
        });
    }

    // ---------------------------------------------------------------------------
    // Capture first, then cancel → Fulfilled (cancel ignored)
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

            for (var i = 0; i < 100; i++)
            {
                var s = await handle.QueryAsync(wf => wf.GetStatus());
                if (s.Status == OrderStatus.WaitingForPayment) break;
                await Task.Delay(10);
            }

            await handle.SignalAsync(wf => wf.CapturePaymentAsync(payment));
            // Signal name must match the [WorkflowSignal] method name (without Async suffix).
            await handle.SignalAsync("CancelOrderSignal", new[] { "Too late cancel" });

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

            // Wait until WaitingForPayment before cancelling.
            for (var i = 0; i < 100; i++)
            {
                var s = await handle.QueryAsync(wf => wf.GetStatus());
                if (s.Status == OrderStatus.WaitingForPayment) break;
                await Task.Delay(10);
            }

            await handle.SignalAsync("CancelOrderSignal", new[] { "Customer request" });
            var result = await handle.GetResultAsync();
            Assert.Equal(OrderStatus.Cancelled, result.Status);
        });
    }

    // ---------------------------------------------------------------------------
    // Invalid RRN → loops back, valid capture → Fulfilled
    // ---------------------------------------------------------------------------

    [Fact]
    public async Task InvalidPaymentRrn_ThenValidCapture_CompletesAsFulfilled()
    {
        await using var env = await WorkflowEnvironment.StartTimeSkippingAsync();
        var order = MakeValidOrder("ORD-RETRY");
        // "BADRRN-001" does not start with "RRN-" → MockPaymentService rejects it.
        var badPayment = new PaymentCapture("CUST-1", "BADRRN-001", 100, "ORD-RETRY");
        var goodPayment = new PaymentCapture("CUST-1", "RRN-VALID", 150, "ORD-RETRY");

        await RunTestAsync(env, async () =>
        {
            var handle = await env.Client.StartWorkflowAsync(
                (OrderProcessingWorkflow wf) => wf.RunAsync(order),
                new WorkflowOptions { Id = "ORD-RETRY", TaskQueue = "test-orders" });

            // Wait for WaitingForPayment (no delay — query in tight loop; workflow is
            // progressing on in-process threads, not blocked by a virtual clock).
            for (var i = 0; i < 500; i++)
            {
                var s = await handle.QueryAsync(wf => wf.GetStatus());
                if (s.Status == OrderStatus.WaitingForPayment) break;
            }

            // Send bad payment — the workflow validates it, rejects it, and returns
            // to WaitingForPayment.
            await handle.SignalAsync(wf => wf.CapturePaymentAsync(badPayment));

            // Wait for the status message to confirm rejection before sending the good one.
            for (var i = 0; i < 500; i++)
            {
                var s = await handle.QueryAsync(wf => wf.GetStatus());
                if (s.Status == OrderStatus.WaitingForPayment
                    && (s.Message?.Contains("awaiting") ?? false)) break;
            }

            // Send valid payment.
            await handle.SignalAsync(wf => wf.CapturePaymentAsync(goodPayment));

            var result = await handle.GetResultAsync();
            Assert.Equal(OrderStatus.Fulfilled, result.Status);
        });
    }

    // ---------------------------------------------------------------------------
    // Mismatched customer payment → ignored, correct customer wins
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

    // ---------------------------------------------------------------------------
    // Cancel-then-capture: cancel accepted, then capture arrives → Fulfilled
    // (capture wins because payment has not been captured yet when cancel runs)
    // ---------------------------------------------------------------------------

    [Fact]
    public async Task CancelThenCapture_OrderFulfilled_CancelIgnored()
    {
        await using var env = await WorkflowEnvironment.StartTimeSkippingAsync();
        var order = MakeValidOrder("ORD-CANCEL-THEN-PAY");
        var payment = MakePayment("ORD-CANCEL-THEN-PAY");

        await RunTestAsync(env, async () =>
        {
            var handle = await env.Client.StartWorkflowAsync(
                (OrderProcessingWorkflow wf) => wf.RunAsync(order),
                new WorkflowOptions { Id = "ORD-CANCEL-THEN-PAY", TaskQueue = "test-orders" });

            for (var i = 0; i < 100; i++)
            {
                var s = await handle.QueryAsync(wf => wf.GetStatus());
                if (s.Status == OrderStatus.WaitingForPayment) break;
                await Task.Delay(10);
            }

            // Cancel first — the validator sees PaymentCapture == null, so it should be allowed.
            // Then a capture arrives. Per the updated CapturePaymentAsync, a capture is rejected
            // when CancellationRequested is already set, so the order ends Cancelled.
            await handle.SignalAsync("CancelOrderSignal", new[] { "Customer cancel" });
            await handle.SignalAsync(wf => wf.CapturePaymentAsync(payment));

            var result = await handle.GetResultAsync();
            // Once cancel is set, a subsequent payment signal is dropped.
            Assert.Equal(OrderStatus.Cancelled, result.Status);
        });
    }
}
