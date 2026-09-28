using Microsoft.AspNetCore.Mvc;
using OMS.Worker.Models;
using OMS.Worker.Workflows;
using Temporalio.Client;

namespace OMS.Api.Controllers;

[ApiController]
[Route("api/orders/{orderId}")]
public sealed class SupportController : ControllerBase
{
    private readonly ITemporalClient temporal;

    public SupportController(ITemporalClient temporal) => this.temporal = temporal;

    [HttpPost("cancel")]
    public async Task<IActionResult> Cancel(string orderId, [FromBody] CancelRequest request)
    {
        try
        {
            var handle = temporal.GetWorkflowHandle<OrderProcessingWorkflow>(orderId);
            await handle.ExecuteUpdateAsync(
                (OrderProcessingWorkflow wf) => wf.CancelOrderUpdateAsync(request.Reason),
                new WorkflowUpdateOptions());
            return Accepted(new { orderId, signal = "CancelOrder" });
        }
        catch (Exception ex) when (ex.Message.Contains("not found", StringComparison.OrdinalIgnoreCase)
            || ex.Message.Contains("Workflow not found", StringComparison.OrdinalIgnoreCase))
        {
            return NotFound(new { error = "Order workflow does not exist." });
        }
        catch (Exception ex) when (ex.Message.Contains("cannot be cancelled", StringComparison.OrdinalIgnoreCase)
            || ex.Message.Contains("not allowed", StringComparison.OrdinalIgnoreCase))
        {
            return Conflict(new { error = ex.Message });
        }
    }

    [HttpPost("support-correction")]
    public async Task<IActionResult> Correct(string orderId, SupportCorrection correction)
    {
        try
        {
            var handle = temporal.GetWorkflowHandle<OrderProcessingWorkflow>(orderId);
            await handle.ExecuteUpdateAsync(
                (OrderProcessingWorkflow wf) => wf.CorrectOrderUpdateAsync(correction),
                new WorkflowUpdateOptions());
            return Accepted(new { orderId, signal = "SupportCorrection" });
        }
        catch (Exception ex) when (ex.Message.Contains("not found", StringComparison.OrdinalIgnoreCase)
            || ex.Message.Contains("Workflow not found", StringComparison.OrdinalIgnoreCase))
        {
            return NotFound(new { error = "Order workflow does not exist." });
        }
        catch (Exception ex) when (ex.Message.Contains("only allowed", StringComparison.OrdinalIgnoreCase)
            || ex.Message.Contains("not allowed", StringComparison.OrdinalIgnoreCase))
        {
            return Conflict(new { error = ex.Message });
        }
    }
}

public sealed record CancelRequest(string Reason);
