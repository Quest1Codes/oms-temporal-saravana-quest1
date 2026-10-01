using System.Text.Json.Serialization;

namespace OMS.Worker.Models;

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum OrderStatus
{
    Submitted,
    ValidationFailed,
    Validated,
    Enriched,
    WaitingForPayment,
    PaymentCaptured,
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

/// <summary>
/// Inbound webhook shape from the payment processor, matching the spec:
/// { "customer_id", "rrn", "amount_cents", "metadata": { "order_id" } }
/// Mapped to <see cref="PaymentCapture"/> before entering the workflow.
/// </summary>
public record PaymentWebhookRequest(
    [property: JsonPropertyName("customer_id")] string CustomerId,
    [property: JsonPropertyName("rrn")] string Rrn,
    [property: JsonPropertyName("amount_cents")] long AmountCents,
    [property: JsonPropertyName("metadata")] PaymentMetadata Metadata)
{
    public PaymentCapture ToCapture(string routeOrderId) =>
        new(CustomerId, Rrn, AmountCents, Metadata?.OrderId ?? routeOrderId);
}

public record PaymentMetadata(
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

/// <summary>
/// The outbound message sent to the fulfillment integration (Kafka in production,
/// mock activity in this POC). Shape matches the spec:
/// customer_id, order_id, payment_details.rrn, items[].item_id/sku_id/brand_code.
/// </summary>
public record FulfillmentMessage(
    [property: JsonPropertyName("customer_id")]  string CustomerId,
    [property: JsonPropertyName("order_id")]     string OrderId,
    [property: JsonPropertyName("payment_details")] FulfillmentPaymentDetails PaymentDetails,
    [property: JsonPropertyName("items")]        IReadOnlyList<FulfillmentItem> Items);

public record FulfillmentPaymentDetails(
    [property: JsonPropertyName("rrn")] string Rrn);

public record FulfillmentItem(
    [property: JsonPropertyName("item_id")]    string ItemId,
    [property: JsonPropertyName("quantity")]   int Quantity,
    [property: JsonPropertyName("sku_id")]     string SkuId,
    [property: JsonPropertyName("brand_code")] string BrandCode);

public record OrderStatusView(
    [property: JsonPropertyName("order_id")] string OrderId,
    [property: JsonPropertyName("status")]   OrderStatus Status,
    [property: JsonPropertyName("message")]  string? Message = null,
    [property: JsonPropertyName("rrn")]      string? Rrn = null,
    [property: JsonPropertyName("customer_id")] string? CustomerId = null,
    [property: JsonPropertyName("items")]    IReadOnlyList<OrderItem>? Items = null);
