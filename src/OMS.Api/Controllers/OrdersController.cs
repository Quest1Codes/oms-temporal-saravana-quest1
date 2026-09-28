using Microsoft.AspNetCore.Mvc;
using OMS.Worker.Models;
using OMS.Worker.Services;
using OMS.Worker.Workflows;
using Temporalio.Client;

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
                new StartWorkflowOptions
                {
                    Id = workflowId,
                    TaskQueue = TemporalConstants.TaskQueue,
                    IdConflictPolicy = WorkflowIdConflictPolicy.UseExisting,
                    IdReusePolicy = WorkflowIdReusePolicy.RejectDuplicate
                });

            return Ok(new { workflowId = handle.Id });
        }
        catch (Exception ex) when (ex.Message.Contains("already started", StringComparison.OrdinalIgnoreCase))
        {
            return Conflict(new { error = "Workflow already exists for this order." });
        }
    }

    [HttpGet("{orderId}")]
    public async Task<IActionResult> Get(string orderId)
    {
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
        catch
        {
            return NotFound();
        }
    }
}
