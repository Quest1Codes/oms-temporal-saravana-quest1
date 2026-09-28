using OMS.Worker.Activities;
using OMS.Worker.Models;
using Temporalio.Common;
using Temporalio.Workflows;

namespace OMS.Worker.Workflows;

[Workflow(VersioningBehavior = VersioningBehavior.Pinned)]
public sealed class OrderProcessingWorkflow
{
    private const int OrderTtlDays = 30;
    private static readonly TimeSpan OrderTtl = TimeSpan.FromDays(OrderTtlDays);
    private static readonly SearchAttributeKey<string> OrderStatusKey = SearchAttributeKey.CreateKeyword("OrderStatus");
    private readonly WorkflowState state = new();

    [WorkflowRun]
    public async Task<OrderStatusView> RunAsync(OrderSubmission submission)
    {
        var orderDeadline = Workflow.UtcNow + OrderTtl;

        await SaveStatusAsync(submission.Order.OrderId, OrderStatus.Submitted);

        while (true)
        {
            var validation = await Workflow.ExecuteActivityAsync(
                (OrderActivities a) => a.ValidateOrderAsync(submission),
                ValidationActivityOptions());

            if (!validation.IsValid)
            {
                await SaveStatusAsync(
                    submission.Order.OrderId,
                    OrderStatus.ValidationFailed,
                    validation.Reason);

                var remaining = orderDeadline - Workflow.UtcNow;
                if (remaining <= TimeSpan.Zero)
                {
                    return await ExpireAsync(submission.Order.OrderId);
                }

                var corrected = await Workflow.WaitConditionAsync(
                    () => state.SupportCorrection != null || state.CancellationRequested,
                    remaining);

                if (!corrected)
                {
                    return await ExpireAsync(submission.Order.OrderId);
                }

                if (state.CancellationRequested)
                {
                    return await CancelAsync(submission.Order.OrderId, "Cancelled while awaiting support correction.");
                }

                submission = submission with
                {
                    Order = submission.Order with { Items = state.SupportCorrection!.Items }
                };
                state.SupportCorrection = null;
                continue;
            }

            break;
        }

        await SaveStatusAsync(submission.Order.OrderId, OrderStatus.Validated);

        state.EnrichedOrder = await Workflow.ExecuteActivityAsync(
            (OrderActivities a) => a.EnrichOrderAsync(submission),
            ActivityOptions());

        await SaveStatusAsync(submission.Order.OrderId, OrderStatus.Enriched);
        await SaveStatusAsync(submission.Order.OrderId, OrderStatus.WaitingForPayment);

        var remainingPaymentWait = orderDeadline - Workflow.UtcNow;
        if (remainingPaymentWait <= TimeSpan.Zero)
        {
            return await ExpireAsync(submission.Order.OrderId);
        }

        var completed = await Workflow.WaitConditionAsync(
            () => state.PaymentCapture != null || state.CancellationRequested,
            remainingPaymentWait);

        if (!completed)
        {
            return await ExpireAsync(submission.Order.OrderId);
        }

        if (state.CancellationRequested && state.PaymentCapture == null)
        {
            return await CancelAsync(submission.Order.OrderId, "Order cancelled before payment capture.");
        }

        var capturedPayment = state.PaymentCapture;
        if (capturedPayment == null)
        {
            return await ExpireAsync(submission.Order.OrderId);
        }

        var paymentValid = await Workflow.ExecuteActivityAsync(
            (OrderActivities a) => a.ValidatePaymentAsync(capturedPayment),
            PaymentActivityOptions());

        if (!paymentValid)
        {
            const string reason = "Payment capture could not be validated.";
            await SaveStatusAsync(
                submission.Order.OrderId,
                OrderStatus.PaymentRejected,
                reason,
                capturedPayment.Rrn);
            return new OrderStatusView(
                submission.Order.OrderId,
                OrderStatus.PaymentRejected,
                reason,
                capturedPayment.Rrn);
        }

        await SaveStatusAsync(
            submission.Order.OrderId,
            OrderStatus.PaymentCaptured,
            "Payment capture validated.",
            capturedPayment.Rrn);

        var fulfillment = await Workflow.ExecuteActivityAsync(
            (OrderActivities a) => a.FulfillAsync(state.EnrichedOrder!, capturedPayment),
            FulfillmentActivityOptions());

        if (!fulfillment.Accepted)
        {
            const string reason = "Fulfillment rejected the order.";
            await SaveStatusAsync(
                submission.Order.OrderId,
                OrderStatus.FulfillmentFailed,
                reason,
                capturedPayment.Rrn);
            return new OrderStatusView(
                submission.Order.OrderId,
                OrderStatus.FulfillmentFailed,
                reason,
                capturedPayment.Rrn);
        }

        try
        {
            await Workflow.ExecuteActivityAsync(
                (OrderActivities a) => a.SaveFulfilledAsync(state.EnrichedOrder!, capturedPayment),
                ActivityOptions());
        }
        catch
        {
            state.Status = OrderStatus.Fulfilled;
            state.Message = "Order forwarded to fulfillment. Dashboard projection sync failed after fulfillment acceptance.";
            return new OrderStatusView(
                submission.Order.OrderId,
                OrderStatus.Fulfilled,
                state.Message,
                capturedPayment.Rrn);
        }

        state.Status = OrderStatus.Fulfilled;
        state.Message = "Order forwarded to fulfillment.";

        return new OrderStatusView(submission.Order.OrderId, state.Status, state.Message, capturedPayment.Rrn);
    }

    [WorkflowSignal]
    public Task CapturePaymentAsync(PaymentCapture capture)
    {
        if (state.PaymentCapture == null && !IsTerminal(state.Status) && state.Status != OrderStatus.PaymentRejected)
        {
            state.PaymentCapture = capture;
        }

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
        if (state.PaymentCapture != null)
        {
            throw new InvalidOperationException("Payment has already been captured for this order.");
        }

        if (state.Status == OrderStatus.Cancelled || state.Status == OrderStatus.Expired || state.Status == OrderStatus.Fulfilled)
        {
            throw new InvalidOperationException("Payment update is not allowed in the current order state.");
        }
    }

    [WorkflowSignal]
    public Task CancelAsync(string reason)
    {
        if (IsTerminal(state.Status) || state.PaymentCapture != null)
        {
            return Task.CompletedTask;
        }

        state.CancellationRequested = true;
        state.Message = reason;
        return Task.CompletedTask;
    }

    [WorkflowUpdate]
    public Task CancelOrderUpdateAsync(string reason)
    {
        return CancelAsync(reason);
    }

    [WorkflowUpdateValidator(nameof(CancelOrderUpdateAsync))]
    public void ValidateCancelOrderUpdate(string reason)
    {
        if (state.PaymentCapture != null)
        {
            throw new InvalidOperationException("Order cannot be cancelled after payment has been captured.");
        }
    }

    [WorkflowSignal]
    public Task CorrectOrderAsync(SupportCorrection correction)
    {
        if (!IsTerminal(state.Status) && state.Status == OrderStatus.ValidationFailed)
        {
            state.SupportCorrection = correction;
        }
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
        {
            throw new InvalidOperationException("Support corrections are only allowed while the order is awaiting validation correction.");
        }
    }

    [WorkflowQuery]
    public OrderStatusView GetStatus()
    {
        return new OrderStatusView(
            Workflow.Info.WorkflowId,
            state.Status,
            state.Message,
            state.PaymentCapture?.Rrn);
    }

    private async Task<OrderStatusView> CancelAsync(string orderId, string reason)
    {
        state.Status = OrderStatus.Cancelled;
        state.Message = reason;
        await SaveStatusAsync(orderId, state.Status, reason);
        return new OrderStatusView(orderId, state.Status, reason, state.PaymentCapture?.Rrn);
    }

    private async Task<OrderStatusView> ExpireAsync(string orderId)
    {
        state.Status = OrderStatus.Expired;
        state.Message = "Payment capture was not received within 30 days.";
        await SaveStatusAsync(orderId, state.Status, state.Message);
        return new OrderStatusView(orderId, state.Status, state.Message);
    }

    private async Task SaveStatusAsync(
        string orderId,
        OrderStatus newStatus,
        string? newMessage = null,
        string? rrn = null)
    {
        state.Status = newStatus;
        state.Message = newMessage;
        Workflow.UpsertTypedSearchAttributes(new[]
        {
            SearchAttributeUpdate.ValueSet(OrderStatusKey, newStatus.ToString())
        });

        await Workflow.ExecuteActivityAsync(
            (OrderActivities a) => a.SaveStatusAsync(
                new OrderStatusView(orderId, newStatus, newMessage, rrn)),
            ActivityOptions());
    }

    private static ActivityOptions ValidationActivityOptions() => new()
    {
        TaskQueue = TemporalConstants.CommerceTaskQueue,
        StartToCloseTimeout = TimeSpan.FromSeconds(15),
        ScheduleToCloseTimeout = TimeSpan.FromMinutes(2)
    };

    private static ActivityOptions ActivityOptions() => new()
    {
        StartToCloseTimeout = TimeSpan.FromSeconds(15),
        ScheduleToCloseTimeout = TimeSpan.FromMinutes(2)
    };

    private static ActivityOptions PaymentActivityOptions() => new()
    {
        StartToCloseTimeout = TimeSpan.FromSeconds(10),
        ScheduleToCloseTimeout = TimeSpan.FromMinutes(1)
    };

    private static ActivityOptions FulfillmentActivityOptions() => new()
    {
        StartToCloseTimeout = TimeSpan.FromSeconds(15),
        ScheduleToCloseTimeout = TimeSpan.FromMinutes(3)
    };

    private static ActivityOptions CompensationActivityOptions() => new()
    {
        StartToCloseTimeout = TimeSpan.FromSeconds(15),
        ScheduleToCloseTimeout = TimeSpan.FromMinutes(2)
    };

    private static bool IsTerminal(OrderStatus orderStatus) =>
        orderStatus is OrderStatus.PaymentCaptured
            or OrderStatus.Cancelled
            or OrderStatus.Expired
            or OrderStatus.FulfillmentFailed
            or OrderStatus.Fulfilled
            or OrderStatus.PaymentRejected;

    private sealed class WorkflowState
    {
        public OrderStatus Status { get; set; } = OrderStatus.Submitted;
        public string? Message { get; set; }
        public PaymentCapture? PaymentCapture { get; set; }
        public SupportCorrection? SupportCorrection { get; set; }
        public bool CancellationRequested { get; set; }
        public EnrichedOrder? EnrichedOrder { get; set; }
    }
}
