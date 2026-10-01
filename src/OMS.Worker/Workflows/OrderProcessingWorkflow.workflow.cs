using OMS.Worker.Activities;
using OMS.Worker.Models;
using Temporalio.Common;
using Temporalio.Exceptions;
using Temporalio.Workflows;

namespace OMS.Worker.Workflows;

[Workflow(VersioningBehavior = VersioningBehavior.Pinned)]
public sealed class OrderProcessingWorkflow
{
    private const int OrderTtlDays = 30;
    private static readonly TimeSpan OrderTtl = TimeSpan.FromDays(OrderTtlDays);
    private static readonly SearchAttributeKey<string> OrderStatusKey =
        SearchAttributeKey.CreateKeyword("OrderStatus");

    private readonly WorkflowState state = new();

    // Pending dashboard-projection tasks started with fire-and-forget semantics.
    // Awaited via WhenAllAsync before each terminal return.
    private readonly List<Task> pendingProjections = new();

    [WorkflowRun]
    public async Task<OrderStatusView> RunAsync(OrderSubmission? submission = null)
    {
        // -----------------------------------------------------------------------
        // Payment-before-order: workflow may be started by the payment controller
        // (via signal-with-start) before the Commerce webhook arrives.  In that
        // case submission is null and we wait for it to arrive as a signal.
        // -----------------------------------------------------------------------
        if (submission == null)
        {
            var arrived = await Workflow.WaitConditionAsync(
                () => state.PendingSubmission != null,
                OrderTtl);

            if (!arrived || state.PendingSubmission == null)
                return await ExpireAsync(Workflow.Info.WorkflowId,
                    "Order submission was never received.");

            submission = state.PendingSubmission;
        }

        var orderDeadline = Workflow.UtcNow + OrderTtl;
        state.CustomerId = submission.CustomerId;

        FireStatusProjection(submission.Order.OrderId, OrderStatus.Submitted);

        // --- Validation + correction loop ---
        while (true)
        {
            // Trim completed projection tasks to keep the list small.
            pendingProjections.RemoveAll(t => t.IsCompleted);

            ValidationResult validation;
            try
            {
                validation = await Workflow.ExecuteActivityAsync(
                    (OrderActivities a) => a.ValidateOrderAsync(submission),
                    ValidationActivityOptions());
            }
            catch (ActivityFailureException)
            {
                // Commerce service unavailable for the full budget window — expire gracefully.
                return await ExpireAsync(submission.Order.OrderId,
                    "Commerce validation service unavailable; order expired.");
            }

            if (!validation.IsValid)
            {
                FireStatusProjection(submission.Order.OrderId,
                    OrderStatus.ValidationFailed, validation.Reason);

                var remaining = orderDeadline - Workflow.UtcNow;
                if (remaining <= TimeSpan.Zero)
                    return await ExpireAsync(submission.Order.OrderId, "Expired waiting for support correction.");

                var corrected = await Workflow.WaitConditionAsync(
                    () => state.SupportCorrection != null || state.CancellationRequested,
                    remaining);

                if (!corrected)
                    return await ExpireAsync(submission.Order.OrderId, "Expired waiting for support correction.");

                if (state.CancellationRequested)
                    return await CancelAsync(submission.Order.OrderId, "Cancelled while awaiting support correction.");

                submission = submission with
                {
                    Order = submission.Order with { Items = state.SupportCorrection!.Items }
                };
                state.SupportCorrection = null;
                continue;
            }

            break;
        }

        FireStatusProjection(submission.Order.OrderId, OrderStatus.Validated);

        state.EnrichedOrder = await Workflow.ExecuteActivityAsync(
            (OrderActivities a) => a.EnrichOrderAsync(submission),
            EnrichmentActivityOptions());

        // Fire-and-forget projections; trim completed tasks before adding new ones.
        pendingProjections.RemoveAll(t => t.IsCompleted);
        FireStatusProjection(submission.Order.OrderId, OrderStatus.Enriched);
        FireStatusProjection(submission.Order.OrderId, OrderStatus.WaitingForPayment);

        // --- Payment wait loop ---
        var remainingPaymentWait = orderDeadline - Workflow.UtcNow;
        if (remainingPaymentWait <= TimeSpan.Zero)
            return await ExpireAsync(submission.Order.OrderId, "Expired before payment wait.");

        var completed = await Workflow.WaitConditionAsync(
            () => state.PaymentCapture != null || state.CancellationRequested,
            remainingPaymentWait);

        if (!completed)
            return await ExpireAsync(submission.Order.OrderId,
                "Payment capture was not received within 30 days.");

        if (state.CancellationRequested && state.PaymentCapture == null)
            return await CancelAsync(submission.Order.OrderId, "Order cancelled before payment capture.");

        while (true)
        {
            // Trim on each iteration to prevent accumulation over many invalid captures.
            pendingProjections.RemoveAll(t => t.IsCompleted);

            var capturedPayment = state.PaymentCapture;
            if (capturedPayment == null)
                return await ExpireAsync(submission.Order.OrderId,
                    "Payment capture was not received within 30 days.");

            bool paymentValid;
            try
            {
                paymentValid = await Workflow.ExecuteActivityAsync(
                    (OrderActivities a) => a.ValidatePaymentAsync(capturedPayment),
                    PaymentActivityOptions());
            }
            catch (ActivityFailureException)
            {
                // Payment processor unreachable for > 1 min: clear capture and park back.
                state.PaymentCapture = null;
                FireStatusProjection(submission.Order.OrderId, OrderStatus.WaitingForPayment,
                    "Payment validation service temporarily unavailable; awaiting a new capture.");
                paymentValid = false;
            }

            if (!paymentValid)
            {
                if (state.PaymentCapture != null)
                {
                    // Explicit invalid RRN — clear and wait for a fresh capture.
                    state.PaymentCapture = null;
                    FireStatusProjection(submission.Order.OrderId, OrderStatus.WaitingForPayment,
                        "Payment capture could not be validated; awaiting a new capture.");
                }

                var remainingPaymentRetry = orderDeadline - Workflow.UtcNow;
                if (remainingPaymentRetry <= TimeSpan.Zero)
                    return await ExpireAsync(submission.Order.OrderId,
                        "Payment capture was not received within 30 days.");

                var retryCompleted = await Workflow.WaitConditionAsync(
                    () => state.PaymentCapture != null || state.CancellationRequested,
                    remainingPaymentRetry);

                if (!retryCompleted)
                    return await ExpireAsync(submission.Order.OrderId,
                        "Payment capture was not received within 30 days.");

                if (state.CancellationRequested && state.PaymentCapture == null)
                    return await CancelAsync(submission.Order.OrderId, "Order cancelled before payment capture.");

                continue;
            }

            // --- Payment is valid → fulfillment ---
            FireStatusProjection(submission.Order.OrderId, OrderStatus.PaymentCaptured,
                "Payment capture validated.", capturedPayment.Rrn);

            var fulfillmentResult = await Workflow.ExecuteActivityAsync(
                (OrderActivities a) => a.FulfillAsync(state.EnrichedOrder!, capturedPayment),
                FulfillmentActivityOptions());

            if (!fulfillmentResult.Accepted)
            {
                return await SetTerminalAsync(submission.Order.OrderId,
                    OrderStatus.FulfillmentFailed, "Fulfillment rejected the order.", capturedPayment.Rrn);
            }

            // Persist full enriched record; treat failure as eventual projection lag.
            try
            {
                await Workflow.ExecuteActivityAsync(
                    (OrderActivities a) => a.SaveFulfilledAsync(state.EnrichedOrder!, capturedPayment),
                    FulfilledDashboardActivityOptions());
            }
            catch (ActivityFailureException)
            {
                // Dashboard write failed after fulfillment accepted — do NOT compensate.
                // Operator can reconcile from the Temporal UI history.
            }

            return await SetTerminalAsync(submission.Order.OrderId,
                OrderStatus.Fulfilled, "Order forwarded to fulfillment.", capturedPayment.Rrn);
        }
    }

    // ---------------------------------------------------------------------------
    // Signal handlers — kept as a documented fallback for integrations that
    // cannot use Updates (fire-and-forget webhooks, etc.).
    // The Update variants provide synchronous accept/reject feedback via HTTP 409.
    // ---------------------------------------------------------------------------

    /// <summary>
    /// Delivers a pending order submission when the workflow was started by the payment
    /// controller before the Commerce webhook arrived (payment-before-order path).
    /// </summary>
    [WorkflowSignal]
    public Task ReceiveOrderSubmissionAsync(OrderSubmission submission)
    {
        if (state.PendingSubmission == null && !IsTerminal(state.Status))
            state.PendingSubmission = submission;
        return Task.CompletedTask;
    }

    [WorkflowSignal]
    public Task CapturePaymentAsync(PaymentCapture capture)
    {
        if (!string.Equals(capture.CustomerId, state.CustomerId, StringComparison.Ordinal))
            return Task.CompletedTask;
        if (state.CancellationRequested || state.PaymentCapture != null || IsTerminal(state.Status))
            return Task.CompletedTask;

        state.PaymentCapture = capture;
        return Task.CompletedTask;
    }

    [WorkflowUpdate]
    public Task CapturePaymentUpdateAsync(PaymentCapture capture)
    {
        return CapturePaymentAsync(capture);
    }

    [WorkflowUpdateValidator(nameof(CapturePaymentUpdateAsync))]
    public void ValidateCapturePaymentUpdate(PaymentCapture capture)
    {
        if (!string.Equals(capture.CustomerId, state.CustomerId, StringComparison.Ordinal))
            throw new ApplicationFailureException(
                "Payment customer does not match the order customer.",
                errorType: "CustomerMismatch", nonRetryable: true);

        if (state.CancellationRequested)
            throw new ApplicationFailureException(
                "Order has already been cancelled.",
                errorType: "OrderCancelled", nonRetryable: true);

        if (state.PaymentCapture != null)
            throw new ApplicationFailureException(
                "Payment has already been captured for this order.",
                errorType: "PaymentAlreadyCaptured", nonRetryable: true);

        if (IsTerminal(state.Status))
            throw new ApplicationFailureException(
                $"Payment update is not allowed in state '{state.Status}'.",
                errorType: "WrongPhase", nonRetryable: true);
    }

    [WorkflowSignal]
    public Task CancelOrderSignalAsync(string reason)
    {
        if (IsTerminal(state.Status) || state.PaymentCapture != null || state.CancellationRequested)
            return Task.CompletedTask;

        state.CancellationRequested = true;
        state.Message = reason;
        return Task.CompletedTask;
    }

    [WorkflowUpdate]
    public Task CancelOrderUpdateAsync(string reason)
    {
        return CancelOrderSignalAsync(reason);
    }

    [WorkflowUpdateValidator(nameof(CancelOrderUpdateAsync))]
    public void ValidateCancelOrderUpdate(string reason)
    {
        if (state.PaymentCapture != null)
            throw new ApplicationFailureException(
                "Order cannot be cancelled after payment has been captured.",
                errorType: "PaymentAlreadyCaptured", nonRetryable: true);

        if (IsTerminal(state.Status))
            throw new ApplicationFailureException(
                $"Order cannot be cancelled in state '{state.Status}'.",
                errorType: "WrongPhase", nonRetryable: true);
    }

    [WorkflowSignal]
    public Task CorrectOrderAsync(SupportCorrection correction)
    {
        if (state.Status == OrderStatus.ValidationFailed)
            state.SupportCorrection = correction;
        return Task.CompletedTask;
    }

    [WorkflowUpdate]
    public Task CorrectOrderUpdateAsync(SupportCorrection correction)
    {
        return CorrectOrderAsync(correction);
    }

    [WorkflowUpdateValidator(nameof(CorrectOrderUpdateAsync))]
    public void ValidateCorrectOrderUpdate(SupportCorrection correction)
    {
        if (state.Status != OrderStatus.ValidationFailed)
            throw new ApplicationFailureException(
                $"Support corrections are only allowed in ValidationFailed state (current: '{state.Status}').",
                errorType: "WrongPhase", nonRetryable: true);
    }

    [WorkflowQuery]
    public OrderStatusView GetStatus() =>
        new(Workflow.Info.WorkflowId, state.Status, state.Message, state.PaymentCapture?.Rrn);

    // ---------------------------------------------------------------------------
    // Private helpers
    // ---------------------------------------------------------------------------

    private async Task<OrderStatusView> SetTerminalAsync(
        string orderId,
        OrderStatus status,
        string message,
        string? rrn = null)
    {
        state.Status = status;
        state.Message = message;
        SetSearchAttribute(status);

        // Flush all pending projection tasks so the dashboard is up to date at close.
        await Workflow.WhenAllAsync(pendingProjections);
        pendingProjections.Clear();

        await Workflow.WaitConditionAsync(() => Workflow.AllHandlersFinished);
        return new OrderStatusView(orderId, status, message, rrn);
    }

    private async Task<OrderStatusView> CancelAsync(string orderId, string reason)
    {
        FireStatusProjection(orderId, OrderStatus.Cancelled, reason);
        return await SetTerminalAsync(orderId, OrderStatus.Cancelled, reason);
    }

    private async Task<OrderStatusView> ExpireAsync(string orderId, string reason)
    {
        FireStatusProjection(orderId, OrderStatus.Expired, reason);
        return await SetTerminalAsync(orderId, OrderStatus.Expired, reason);
    }

    private void FireStatusProjection(
        string orderId,
        OrderStatus newStatus,
        string? message = null,
        string? rrn = null)
    {
        state.Status = newStatus;
        state.Message = message;
        SetSearchAttribute(newStatus);
        var view = new OrderStatusView(orderId, newStatus, message, rrn);
        pendingProjections.Add(
            Workflow.ExecuteActivityAsync(
                (OrderActivities a) => a.SaveStatusAsync(view),
                StatusActivityOptions()));
    }

    private void SetSearchAttribute(OrderStatus status)
    {
        Workflow.UpsertTypedSearchAttributes(
            SearchAttributeUpdate.ValueSet(OrderStatusKey, status.ToString()));
    }

    // ---------------------------------------------------------------------------
    // Activity options
    // ---------------------------------------------------------------------------

    private static ActivityOptions ValidationActivityOptions() => new()
    {
        TaskQueue = TemporalConstants.CommerceTaskQueue,
        StartToCloseTimeout = TimeSpan.FromSeconds(15),
        ScheduleToCloseTimeout = TimeSpan.FromMinutes(2)
    };

    private static ActivityOptions EnrichmentActivityOptions() => new()
    {
        StartToCloseTimeout = TimeSpan.FromSeconds(30),
        ScheduleToCloseTimeout = TimeSpan.FromHours(4),
        RetryPolicy = new RetryPolicy { MaximumInterval = TimeSpan.FromMinutes(5) }
    };

    private static ActivityOptions StatusActivityOptions() => new()
    {
        StartToCloseTimeout = TimeSpan.FromSeconds(15),
        RetryPolicy = new RetryPolicy { MaximumInterval = TimeSpan.FromMinutes(5) }
    };

    private static ActivityOptions PaymentActivityOptions() => new()
    {
        StartToCloseTimeout = TimeSpan.FromSeconds(10),
        ScheduleToCloseTimeout = TimeSpan.FromMinutes(1)
    };

    private static ActivityOptions FulfillmentActivityOptions() => new()
    {
        StartToCloseTimeout = TimeSpan.FromSeconds(30),
        ScheduleToCloseTimeout = TimeSpan.FromHours(4),
        RetryPolicy = new RetryPolicy { MaximumInterval = TimeSpan.FromMinutes(5) }
    };

    private static ActivityOptions FulfilledDashboardActivityOptions() => new()
    {
        StartToCloseTimeout = TimeSpan.FromSeconds(15),
        RetryPolicy = new RetryPolicy { MaximumInterval = TimeSpan.FromMinutes(5) }
    };

    private static bool IsTerminal(OrderStatus status) =>
        status is OrderStatus.Cancelled
            or OrderStatus.Expired
            or OrderStatus.FulfillmentFailed
            or OrderStatus.Fulfilled;

    private sealed class WorkflowState
    {
        public string CustomerId { get; set; } = string.Empty;
        public OrderStatus Status { get; set; } = OrderStatus.Submitted;
        public string? Message { get; set; }
        public PaymentCapture? PaymentCapture { get; set; }
        public OrderSubmission? PendingSubmission { get; set; }
        public SupportCorrection? SupportCorrection { get; set; }
        public bool CancellationRequested { get; set; }
        public EnrichedOrder? EnrichedOrder { get; set; }
    }
}
