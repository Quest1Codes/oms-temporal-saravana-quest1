using System.Text.Json.Serialization;

namespace OMS.Worker.Models;

public enum OrderStatus
{
    Submitted,
    ValidationFailed,
    Validated,
    Enriched,
    WaitingForPayment,
    PaymentCaptured,
    PaymentRejected,
    Cancelled,
    Expired,
    FulfillmentFailed,
    Fulfilled
}

public record OrderSubmission(
    [property: JsonPropertyName("customer_id")] string CustomerId,
    [property: JsonPropertyName("order")] OrderPayload Order,
    [property: JsonPropertyName("risk_data")] RiskData? RiskData = null);

public record OrderPayload(
    [property: JsonPropertyName("order_id")] string OrderId,
    [property: JsonPropertyName("items")] IReadOnlyList<OrderItem> Items);

public record OrderItem(
    [property: JsonPropertyName("item_id")] string ItemId,
    [property: JsonPropertyName("quantity")] int Quantity,
    [property: JsonPropertyName("sku_id")] string? SkuId = null,
    [property: JsonPropertyName("brand_code")] string? BrandCode = null);

public record RiskData(
    [property: JsonPropertyName("risk_input")] string? RiskInput,
    [property: JsonPropertyName("risk_decision")] string? RiskDecision = null);

public record PaymentCapture(
    [property: JsonPropertyName("customer_id")] string CustomerId,
    [property: JsonPropertyName("rrn")] string Rrn,
    [property: JsonPropertyName("amount_cents")] long AmountCents,
    [property: JsonPropertyName("order_id")] string OrderId);

public record SupportCorrection(
    [property: JsonPropertyName("items")] IReadOnlyList<OrderItem> Items);

public record ValidationResult(
    [property: JsonPropertyName("is_valid")] bool IsValid,
    [property: JsonPropertyName("reason")] string? Reason = null);

public record EnrichedOrder(
    [property: JsonPropertyName("customer_id")] string CustomerId,
    [property: JsonPropertyName("order_id")] string OrderId,
    [property: JsonPropertyName("items")] IReadOnlyList<OrderItem> Items);

public record FulfillmentResult(
    [property: JsonPropertyName("order_id")] string OrderId,
    [property: JsonPropertyName("accepted")] bool Accepted);

public record OrderStatusView(
    [property: JsonPropertyName("order_id")] string OrderId,
    [property: JsonPropertyName("status")] OrderStatus Status,
    [property: JsonPropertyName("message")] string? Message = null,
    [property: JsonPropertyName("rrn")] string? Rrn = null);
