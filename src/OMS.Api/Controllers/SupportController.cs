using Microsoft.AspNetCore.Mvc;
using OMS.Worker.Models;
using OMS.Worker.Workflows;
using Temporalio.Client;
using Temporalio.Exceptions;

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
            return Accepted(new { orderId, message = "Cancellation signal sent" });
        }
        catch (WorkflowUpdateFailedException e)
            when (e.InnerException is ApplicationFailureException af)
        {
            // Validator rejected: payment already captured, terminal state, etc.
            return Conflict(new { error = af.Message, type = af.ErrorType });
        }
        catch (RpcException rpc) when (rpc.Code == RpcException.StatusCode.NotFound)
        {
            return NotFound(new { error = $"Order '{orderId}' not found." });
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

    [HttpPost("support-correction")]
    public async Task<IActionResult> Correct(string orderId, SupportCorrection correction)
    {
        try
        {
            var handle = temporal.GetWorkflowHandle<OrderProcessingWorkflow>(orderId);
            await handle.ExecuteUpdateAsync(
                (OrderProcessingWorkflow wf) => wf.CorrectOrderUpdateAsync(correction),
                new WorkflowUpdateOptions());
            return Accepted(new { orderId, message = "Support correction signal sent" });
        }
        catch (WorkflowUpdateFailedException e)
            when (e.InnerException is ApplicationFailureException af)
        {
            // Validator rejected: order is not in ValidationFailed state.
            return Conflict(new { error = af.Message, type = af.ErrorType });
        }
        catch (RpcException rpc) when (rpc.Code == RpcException.StatusCode.NotFound)
        {
            return NotFound(new { error = $"Order '{orderId}' not found." });
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

public sealed record CancelRequest(string Reason);
