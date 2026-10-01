using Microsoft.AspNetCore.Mvc;
using OMS.Worker.Models;
using OMS.Worker.Services;
using OMS.Worker.Workflows;
using Temporalio.Client;
using Temporalio.Exceptions;

namespace OMS.Api.Controllers;

[ApiController]
[Route("api/orders")]
public sealed class OrdersController : ControllerBase
{
    private readonly ITemporalClient temporal;
    private readonly IOrderRepository repository;

    public OrdersController(ITemporalClient temporal, IOrderRepository repository)
    {
        this.temporal = temporal;
        this.repository = repository;
    }

    [HttpPost]
    public async Task<IActionResult> Submit(OrderSubmission request)
    {
        var workflowId = request.Order.OrderId;

        try
        {
            var handle = await temporal.StartWorkflowAsync(
                (OrderProcessingWorkflow wf) => wf.RunAsync(request),
                new WorkflowOptions(id: workflowId, taskQueue: TemporalConstants.TaskQueue)
                {
                    IdConflictPolicy = WorkflowIdConflictPolicy.UseExisting,
                    IdReusePolicy = WorkflowIdReusePolicy.RejectDuplicate
                });

            // If the workflow was already started by the payment controller
            // (payment-before-order), RunAsync received a null submission.
            // Deliver the submission now via the dedicated signal so the workflow
            // can proceed from its WaitConditionAsync.
            // If the workflow was freshly started above with the submission, this
            // signal is harmless — ReceiveOrderSubmissionAsync is a no-op once
            // PendingSubmission is already set.
            await handle.SignalAsync(wf => wf.ReceiveOrderSubmissionAsync(request));

            return Ok(new { workflowId = handle.Id });
        }
        catch (WorkflowAlreadyStartedException)
        {
            return Conflict(new { error = "An order with this ID has already been processed." });
        }
        catch (RpcException rpc)
        {
            return StatusCode(503, new { error = "Order service temporarily unavailable.", detail = rpc.Message });
        }
    }

    [HttpGet("{orderId}")]
    public async Task<IActionResult> Get(string orderId)
    {
        // Check the local projection first — avoids a Temporal round-trip for terminal orders.
        var local = repository.Get(orderId);
        if (local != null)
        {
            return Ok(local);
        }

        try
        {
            var handle = temporal.GetWorkflowHandle<OrderProcessingWorkflow>(orderId);
            var status = await handle.QueryAsync(wf => wf.GetStatus());
            return Ok(status);
        }
        catch (RpcException rpc) when (rpc.Code == RpcException.StatusCode.NotFound)
        {
            return NotFound(new { error = $"Order '{orderId}' not found." });
        }
        catch (RpcException rpc)
        {
            // Temporal service is unreachable or the query failed — not the same as "not found".
            return StatusCode(503, new { error = "Order service temporarily unavailable.", detail = rpc.Message });
        }
    }
}
