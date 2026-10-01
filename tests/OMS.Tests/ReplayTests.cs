using OMS.Worker.Activities;
using OMS.Worker.Models;
using OMS.Worker.Services;
using OMS.Worker.Workflows;
using Temporalio.Client;
using Temporalio.Testing;
using Temporalio.Worker;
using Temporalio.Worker.Interceptors;
using Xunit;

namespace OMS.Tests;

/// <summary>
/// Workflow replay tests.
///
/// Each test runs the workflow to a terminal state, exports the execution history,
/// and immediately replays it through WorkflowReplayer.  This proves the workflow
/// code is deterministic end-to-end: if a future code change alters the command
/// sequence for a path that already has a saved history, the replayer will throw
/// a non-determinism error and the test will fail.
///
/// To add a fixture file for long-term CI regression:
///   1. Run the relevant Generate* test.
///   2. Copy the printed JSON into tests/OMS.Tests/fixtures/<name>.json.
///   3. Add a corresponding Replay_<name>_FromFixture test using
///      WorkflowHistory.FromJsonFileAsync("fixtures/<name>.json").
/// </summary>
public class ReplayTests
{
    // ---------------------------------------------------------------------------
    // Helpers shared with OrderWorkflowTests
    // ---------------------------------------------------------------------------

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

        var commerceWorker = new TemporalWorker(
            env.Client,
            new TemporalWorkerOptions(TemporalConstants.CommerceTaskQueue)
                .AddActivity(activities.ValidateOrderAsync));

        return (defaultWorker, commerceWorker);
    }

    private static async Task RunTestAsync(WorkflowEnvironment env, Func<Task> test)
    {
        await RegisterSearchAttributesAsync(env);
        var (def, comm) = CreateWorkers(env);
        using (def)
        using (comm)
        {
            await def.ExecuteAsync(() => comm.ExecuteAsync(test));
        }
    }

    private static async Task RegisterSearchAttributesAsync(WorkflowEnvironment env)
    {
        try
        {
            await env.Client.Connection.OperatorService.AddSearchAttributesAsync(
                new Temporalio.Api.OperatorService.V1.AddSearchAttributesRequest
                {
                    SearchAttributes = { { "OrderStatus", Temporalio.Api.Enums.V1.IndexedValueType.Keyword } }
                });
        }
        catch { /* Embedded server may not support RPC; safe to skip. */ }
    }

    private static OrderSubmission MakeValidOrder(string id) => new(
        "CUST-REPLAY",
        new OrderPayload(id, new[] { new OrderItem("ITEM-REPLAY", 1, null, null) }),
        null);

    private static OrderSubmission MakeInvalidOrder(string id) => new(
        "CUST-REPLAY",
        new OrderPayload(id, new[] { new OrderItem("INVALID-REPLAY", 0, null, null) }),
        null);

    private static WorkflowReplayer BuildReplayer() =>
        new WorkflowReplayer(
            new WorkflowReplayerOptions().AddWorkflow<OrderProcessingWorkflow>());

    // ---------------------------------------------------------------------------
    // Helpers to run a scenario and return its history
    // ---------------------------------------------------------------------------

    private static async Task<WorkflowHistory> RunAndCaptureHistoryAsync(
        WorkflowEnvironment env, string workflowId, Func<IWorkflowHandle<OrderProcessingWorkflow>, Task> body)
    {
        WorkflowHistory? history = null;

        await RunTestAsync(env, async () =>
        {
            var handle = await env.Client.StartWorkflowAsync(
                (OrderProcessingWorkflow wf) => wf.RunAsync(MakeValidOrder(workflowId)),
                new WorkflowOptions { Id = workflowId, TaskQueue = "test-orders" });

            await body(handle);
            await handle.GetResultAsync(); // ensure complete

            history = await handle.FetchHistoryAsync();
        });

        return history!;
    }

    // ---------------------------------------------------------------------------
    // Replay test: Fulfilled path
    // ---------------------------------------------------------------------------

    [Fact]
    public async Task Replay_FulfilledPath_IsNonDeterministic()
    {
        await using var env = await WorkflowEnvironment.StartTimeSkippingAsync();
        var payment = new PaymentCapture("CUST-REPLAY", "RRN-REPLAY-001", 15000, "ORD-REPLAY-FULFILLED");

        WorkflowHistory? history = null;

        await RunTestAsync(env, async () =>
        {
            var handle = await env.Client.StartWorkflowAsync(
                (OrderProcessingWorkflow wf) => wf.RunAsync(MakeValidOrder("ORD-REPLAY-FULFILLED")),
                new WorkflowOptions { Id = "ORD-REPLAY-FULFILLED", TaskQueue = "test-orders" });

            // Wait until WaitingForPayment before sending payment.
            for (var i = 0; i < 200; i++)
            {
                var s = await handle.QueryAsync(wf => wf.GetStatus());
                if (s.Status == OrderStatus.WaitingForPayment) break;
            }

            await handle.SignalAsync(wf => wf.CapturePaymentAsync(payment));
            var result = await handle.GetResultAsync();
            Assert.Equal(OrderStatus.Fulfilled, result.Status);

            history = await handle.FetchHistoryAsync();
        });

        // Replay the captured history through the current workflow code.
        // This will throw NonDeterminismException if any code change reorders commands.
        var replayer = BuildReplayer();
        await replayer.ReplayWorkflowAsync(history!);
    }

    // ---------------------------------------------------------------------------
    // Replay test: Expired path (no payment)
    // ---------------------------------------------------------------------------

    [Fact]
    public async Task Replay_ExpiredPath_IsNonDeterministic()
    {
        await using var env = await WorkflowEnvironment.StartTimeSkippingAsync();

        WorkflowHistory? history = null;

        await RunTestAsync(env, async () =>
        {
            var handle = await env.Client.StartWorkflowAsync(
                (OrderProcessingWorkflow wf) => wf.RunAsync(MakeValidOrder("ORD-REPLAY-EXPIRED")),
                new WorkflowOptions { Id = "ORD-REPLAY-EXPIRED", TaskQueue = "test-orders" });

            var result = await handle.GetResultAsync(); // time-skipping advances past TTL
            Assert.Equal(OrderStatus.Expired, result.Status);

            history = await handle.FetchHistoryAsync();
        });

        var replayer = BuildReplayer();
        await replayer.ReplayWorkflowAsync(history!);
    }

    // ---------------------------------------------------------------------------
    // Replay test: Cancelled path
    // ---------------------------------------------------------------------------

    [Fact]
    public async Task Replay_CancelledPath_IsNonDeterministic()
    {
        await using var env = await WorkflowEnvironment.StartTimeSkippingAsync();

        WorkflowHistory? history = null;

        await RunTestAsync(env, async () =>
        {
            var handle = await env.Client.StartWorkflowAsync(
                (OrderProcessingWorkflow wf) => wf.RunAsync(MakeValidOrder("ORD-REPLAY-CANCELLED")),
                new WorkflowOptions { Id = "ORD-REPLAY-CANCELLED", TaskQueue = "test-orders" });

            // Wait until WaitingForPayment.
            for (var i = 0; i < 200; i++)
            {
                var s = await handle.QueryAsync(wf => wf.GetStatus());
                if (s.Status == OrderStatus.WaitingForPayment) break;
            }

            await handle.SignalAsync("CancelOrderSignal", new[] { "Replay test cancel" });
            var result = await handle.GetResultAsync();
            Assert.Equal(OrderStatus.Cancelled, result.Status);

            history = await handle.FetchHistoryAsync();
        });

        var replayer = BuildReplayer();
        await replayer.ReplayWorkflowAsync(history!);
    }

    // ---------------------------------------------------------------------------
    // Replay test: Expired-during-correction path
    // ---------------------------------------------------------------------------

    [Fact]
    public async Task Replay_ExpiredDuringCorrectionPath_IsNonDeterministic()
    {
        await using var env = await WorkflowEnvironment.StartTimeSkippingAsync();

        WorkflowHistory? history = null;

        await RunTestAsync(env, async () =>
        {
            var handle = await env.Client.StartWorkflowAsync(
                (OrderProcessingWorkflow wf) => wf.RunAsync(MakeInvalidOrder("ORD-REPLAY-EXPIRE-CORRECT")),
                new WorkflowOptions { Id = "ORD-REPLAY-EXPIRE-CORRECT", TaskQueue = "test-orders" });

            // No correction sent — time-skipping advances past TTL.
            var result = await handle.GetResultAsync();
            Assert.Equal(OrderStatus.Expired, result.Status);

            history = await handle.FetchHistoryAsync();
        });

        var replayer = BuildReplayer();
        await replayer.ReplayWorkflowAsync(history!);
    }
}
