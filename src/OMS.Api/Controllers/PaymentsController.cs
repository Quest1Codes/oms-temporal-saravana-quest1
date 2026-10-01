using Microsoft.AspNetCore.Mvc;
using OMS.Worker.Models;
using OMS.Worker.Workflows;
using Temporalio.Client;
using Temporalio.Exceptions;

namespace OMS.Api.Controllers;

[ApiController]
[Route("api/orders/{orderId}/payment")]
public sealed class PaymentsController : ControllerBase
{
    private readonly ITemporalClient temporal;

    public PaymentsController(ITemporalClient temporal) => this.temporal = temporal;

    [HttpPost]
    public async Task<IActionResult> Capture(string orderId, PaymentWebhookRequest request)
    {
        var capture = request.ToCapture(orderId);

        if (!string.Equals(orderId, capture.OrderId, StringComparison.Ordinal))
        {
            return BadRequest(new { error = "Order ID in the route and payload must match." });
        }

        try
        {
            var handle = temporal.GetWorkflowHandle<OrderProcessingWorkflow>(orderId);
            await handle.ExecuteUpdateAsync(
                (OrderProcessingWorkflow wf) => wf.CapturePaymentUpdateAsync(capture),
                new WorkflowUpdateOptions());
            return Accepted(new { orderId, message = "Payment signal sent" });
        }
        catch (WorkflowUpdateFailedException e)
            when (e.InnerException is ApplicationFailureException af)
        {
            // Validator rejected: wrong customer, duplicate capture, wrong phase, etc.
            return Conflict(new { error = af.Message, type = af.ErrorType });
        }
        catch (RpcException rpc) when (rpc.Code == RpcException.StatusCode.NotFound
            || rpc.Message.Contains("workflow not found", StringComparison.OrdinalIgnoreCase))
        {
            // Workflow does not exist yet — payment arrived before the Commerce webhook.
            // Start the workflow in a "waiting for submission" state and deliver the
            // payment as a buffered signal.  When the Commerce webhook arrives it calls
            // ReceiveOrderSubmissionAsync, the workflow unparks, and RunAsync proceeds
            // with the submission — by which point the payment is already in state.
            try
            {
                await temporal.StartWorkflowAsync(
                    (OrderProcessingWorkflow wf) => wf.RunAsync(null),
                    new WorkflowOptions(id: orderId, taskQueue: TemporalConstants.TaskQueue)
                    {
                        IdConflictPolicy = WorkflowIdConflictPolicy.UseExisting,
                        StartSignal = "CapturePaymentAsync",
                        StartSignalArgs = new object[] { capture }
                    });
                return Accepted(new { orderId, message = "Payment buffered; awaiting order submission." });
            }
            catch (Exception ex)
            {
                return StatusCode(503, new { error = "Unable to buffer payment.", detail = ex.Message });
            }
        }
        catch (RpcException rpc) when (rpc.Message.Contains("workflow execution already completed",
            StringComparison.OrdinalIgnoreCase))
        {
            return Conflict(new { error = "Order is already in a terminal state." });
        }
        catch (RpcException rpc)
        {
            return StatusCode(503, new { error = "Order service temporarily unavailable.", detail = rpc.Message });
        }
    }
}
