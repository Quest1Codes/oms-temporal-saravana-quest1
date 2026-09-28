using Microsoft.AspNetCore.Mvc;
using OMS.Worker.Models;
using OMS.Worker.Workflows;
using Temporalio.Client;

namespace OMS.Api.Controllers;

[ApiController]
[Route("api/orders/{orderId}/payment")]
public sealed class PaymentsController : ControllerBase
{
    private readonly ITemporalClient temporal;

    public PaymentsController(ITemporalClient temporal) => this.temporal = temporal;

    [HttpPost]
    public async Task<IActionResult> Capture(string orderId, PaymentCapture request)
    {
        if (!string.Equals(orderId, request.OrderId, StringComparison.Ordinal))
        {
            return BadRequest("Order ID in the route and payload must match.");
        }

        try
        {
            var handle = temporal.GetWorkflowHandle<OrderProcessingWorkflow>(orderId);
            await handle.ExecuteUpdateAsync(
                (OrderProcessingWorkflow wf) => wf.CapturePaymentUpdateAsync(request),
                new WorkflowUpdateOptions());
            return Accepted(new { orderId, signal = "PaymentCaptured" });
        }
        catch (Exception ex) when (ex.Message.Contains("not found", StringComparison.OrdinalIgnoreCase)
            || ex.Message.Contains("Workflow not found", StringComparison.OrdinalIgnoreCase))
        {
            return NotFound(new { error = "Order workflow does not exist." });
        }
        catch (Exception ex) when (ex.Message.Contains("allowed", StringComparison.OrdinalIgnoreCase)
            || ex.Message.Contains("already been captured", StringComparison.OrdinalIgnoreCase))
        {
            return Conflict(new { error = ex.Message });
        }
    }
}
