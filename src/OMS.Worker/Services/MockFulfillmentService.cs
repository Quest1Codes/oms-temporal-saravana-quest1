using System.Collections.Concurrent;
using OMS.Worker.Models;

namespace OMS.Worker.Services;

public sealed class MockFulfillmentService
{
    // Keyed by order ID so idempotent re-submission returns the same result.
    private readonly ConcurrentDictionary<string, FulfillmentMessage> submittedOrders = new();

    /// <summary>
    /// Accepts a strongly-typed <see cref="FulfillmentMessage"/> whose shape matches
    /// the spec output contract: customer_id, order_id, payment_details.rrn,
    /// items[].item_id/sku_id/brand_code. Idempotent by order ID.
    /// </summary>
    public Task<FulfillmentResult> SubmitAsync(FulfillmentMessage message)
    {
        submittedOrders.TryAdd(message.OrderId, message);
        return Task.FromResult(new FulfillmentResult(message.OrderId, Accepted: true));
    }

    public Task<bool> CancelAsync(string orderId)
    {
        return Task.FromResult(submittedOrders.TryRemove(orderId, out _));
    }
}
