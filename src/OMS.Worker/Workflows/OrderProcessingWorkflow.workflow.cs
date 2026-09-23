using OMS.Worker.Activities;
using OMS.Worker.Models;
using Temporalio.Workflows;

namespace OMS.Worker.Workflows;

[Workflow]
public sealed class OrderProcessingWorkflow
{
    private readonly WorkflowState state = new();

    [WorkflowRun]
    public async Task<OrderStatusView> RunAsync(OrderSubmission submission)
    {
        await SaveStatusAsync(submission.Order.OrderId, OrderStatus.Submitted);

        while (true)
        {
            var validation = await Workflow.ExecuteActivityAsync(
                (OrderActivities a) => a.ValidateOrderAsync(submission),
                ActivityOptions());

            if (!validation.IsValid)
            {
                await SaveStatusAsync(
                    submission.Order.OrderId,
                    OrderStatus.ValidationFailed,
                    validation.Reason);

                await Workflow.WaitConditionAsync(
                    () => state.SupportCorrection != null || state.CancellationRequested);

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

        var completed = await Workflow.WaitConditionAsync(
            () => state.PaymentCapture != null || state.CancellationRequested,
            TimeSpan.FromDays(30));

        if (!completed)
        {
            return await ExpireAsync(submission.Order.OrderId);
        }

        if (state.CancellationRequested)
        {
            return await CancelAsync(submission.Order.OrderId, "Order cancelled before payment capture.");
        }

        var capturedPayment = state.PaymentCapture!;
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

        if (state.CancellationRequested)
        {
            return await CancelAsync(submission.Order.OrderId, "Order cancelled before fulfillment.");
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
            await Workflow.ExecuteActivityAsync(
                (OrderActivities a) => a.CompensateFulfillmentAsync(submission.Order.OrderId),
                CompensationActivityOptions());
            throw;
        }

        state.Status = OrderStatus.Fulfilled;
        state.Message = "Order forwarded to fulfillment.";

        return new OrderStatusView(submission.Order.OrderId, state.Status, state.Message, capturedPayment.Rrn);
    }

    [WorkflowSignal]
    public Task CapturePaymentAsync(PaymentCapture capture)
    {
        if (state.PaymentCapture == null && !IsTerminal(state.Status))
        {
            state.PaymentCapture = capture;
        }

        return Task.CompletedTask;
    }

    [WorkflowSignal]
    public Task CancelAsync(string reason)
    {
        if (!IsTerminal(state.Status))
        {
            state.CancellationRequested = true;
            state.Message = reason;
        }

        return Task.CompletedTask;
    }

    [WorkflowSignal]
    public Task CorrectOrderAsync(SupportCorrection correction)
    {
        if (!IsTerminal(state.Status))
        {
            state.SupportCorrection = correction;
        }
        return Task.CompletedTask;
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
        await Workflow.ExecuteActivityAsync(
            (OrderActivities a) => a.SaveStatusAsync(
                new OrderStatusView(orderId, newStatus, newMessage, rrn)),
            ActivityOptions());
    }

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
