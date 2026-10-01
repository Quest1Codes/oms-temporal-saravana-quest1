# OMS Temporal POC - API Payloads

All request bodies use **snake_case** field names matching the spec DTOs.

## Base URL

```text
http://localhost:5000
```

Replace `5000` with the port shown by `dotnet run`.

## Endpoints

| Method | Endpoint | Purpose |
|---|---|---|
| POST | `/api/orders` | Submit an order and start the Temporal workflow |
| GET | `/api/orders/{orderId}` | Get order status |
| POST | `/api/orders/{orderId}/payment` | Send payment capture |
| POST | `/api/orders/{orderId}/cancel` | Cancel an order |
| POST | `/api/orders/{orderId}/support-correction` | Correct an invalid order |
| GET | `/health` | API health check |

---

# 1. Create Order

```http
POST /api/orders
Content-Type: application/json
```

```json
{
  "customer_id": "CUST-001",
  "order": {
    "order_id": "ORD-1001",
    "items": [
      {
        "item_id": "ITEM-001",
        "quantity": 2
      }
    ]
  }
}
```

Expected response:

```json
{
  "workflowId": "ORD-1001"
}
```

The workflow ID is the order ID.

---

# 2. Get Order Status

```http
GET /api/orders/ORD-1001
```

Example response:

```json
{
  "order_id": "ORD-1001",
  "status": "WaitingForPayment"
}
```

Possible statuses:

```text
Submitted
ValidationFailed
Validated
Enriched
WaitingForPayment
PaymentCaptured
Cancelled
Expired
FulfillmentFailed
Fulfilled
```

---

# 3. Capture Payment

Payment may arrive later after order submission.

```http
POST /api/orders/ORD-1001/payment
Content-Type: application/json
```

Payload matches the spec's Payment Processor Service shape:

```json
{
  "customer_id": "CUST-001",
  "rrn": "RRN-100001",
  "amount_cents": 12999,
  "metadata": {
    "order_id": "ORD-1001"
  }
}
```

`metadata.order_id` must match the route `{orderId}`.

Expected response:

```json
{
  "orderId": "ORD-1001",
  "message": "Payment signal sent"
}
```

If payment arrives before the order exists, the response is:

```json
{
  "orderId": "ORD-1001",
  "message": "Payment buffered; awaiting order submission."
}
```

The workflow starts in a waiting state; the payment is delivered once the Commerce webhook arrives.

---

# 4. Cancel Order

Cancellation is allowed before payment capture. Returns HTTP 409 if payment has already been captured or the order is terminal.

```http
POST /api/orders/ORD-1001/cancel
Content-Type: application/json
```

```json
{
  "reason": "Customer requested cancellation"
}
```

Expected response:

```json
{
  "orderId": "ORD-1001",
  "message": "Cancellation signal sent"
}
```

---

# 5. Support Correction

Use this when Commerce validation fails (status `ValidationFailed`). Returns HTTP 409 if the order is not in `ValidationFailed` state.

```http
POST /api/orders/ORD-1002/support-correction
Content-Type: application/json
```

```json
{
  "items": [
    {
      "item_id": "ITEM-001",
      "quantity": 2
    }
  ]
}
```

Expected response:

```json
{
  "orderId": "ORD-1002",
  "message": "Support correction signal sent"
}
```

The workflow receives the correction and re-runs validation.

---

# 6. Health Check

```http
GET /health
```

Example response (Temporal reachable):

```json
{
  "status": "ok",
  "temporal": "localhost:7233"
}
```

Example response (Temporal unreachable — HTTP 503):

```json
{
  "status": "degraded",
  "temporal": "localhost:7233",
  "error": "connection refused"
}
```

---

# End-to-End Scenarios

## Scenario 1 — Normal Order

### Step 1 — Submit

```http
POST /api/orders
Content-Type: application/json
```

```json
{
  "customer_id": "CUST-001",
  "order": {
    "order_id": "ORD-1001",
    "items": [
      { "item_id": "ITEM-001", "quantity": 2 }
    ]
  }
}
```

### Step 2 — Check status

```http
GET /api/orders/ORD-1001
```

Expected: `"status": "WaitingForPayment"`

### Step 3 — Capture payment

```http
POST /api/orders/ORD-1001/payment
Content-Type: application/json
```

```json
{
  "customer_id": "CUST-001",
  "rrn": "RRN-100001",
  "amount_cents": 12999,
  "metadata": { "order_id": "ORD-1001" }
}
```

### Step 4 — Check status

```http
GET /api/orders/ORD-1001
```

Expected final status: `"status": "Fulfilled"`

### Workflow path

```text
Submitted → Commerce Validation → PIM Enrichment
  → WaitingForPayment → [Payment Signal]
  → Payment Validation → Fulfillment → Fulfilled
```

---

## Scenario 2 — Invalid Order and Support Correction

### Step 1 — Submit invalid order

```http
POST /api/orders
Content-Type: application/json
```

```json
{
  "customer_id": "CUST-001",
  "order": {
    "order_id": "ORD-1002",
    "items": [
      { "item_id": "INVALID-ITEM", "quantity": 0 }
    ]
  }
}
```

Expected status: `"status": "ValidationFailed"`

### Step 2 — Correct the order

```http
POST /api/orders/ORD-1002/support-correction
Content-Type: application/json
```

```json
{
  "items": [
    { "item_id": "ITEM-001", "quantity": 1 }
  ]
}
```

Expected progression: `ValidationFailed → Validated → Enriched → WaitingForPayment`

---

## Scenario 3 — Cancellation Before Payment

Submit an order and wait until `WaitingForPayment`, then:

```http
POST /api/orders/ORD-1003/cancel
Content-Type: application/json
```

```json
{ "reason": "Customer cancelled order" }
```

Expected: `"status": "Cancelled"`

---

## Scenario 4 — Payment Never Received (Expires)

Submit an order and do not send payment. After 30 days the workflow expires:

Expected: `"status": "Expired"`

For automated tests, Temporal time-skipping advances the virtual clock instantly.

---

## Scenario 5 — Invalid Payment RRN

Send a payment with an RRN that does not start with `RRN-`:

```json
{
  "customer_id": "CUST-001",
  "rrn": "BADRRN-001",
  "amount_cents": 12999,
  "metadata": { "order_id": "ORD-1005" }
}
```

The workflow rejects the RRN, clears the capture, and returns to `WaitingForPayment` with message `"Payment capture could not be validated; awaiting a new capture."` A subsequent valid capture proceeds normally.

---

## Scenario 6 — Payment Arrives Before Order

Send payment before submitting the order:

```http
POST /api/orders/ORD-1006/payment
Content-Type: application/json
```

```json
{
  "customer_id": "CUST-001",
  "rrn": "RRN-100006",
  "amount_cents": 12999,
  "metadata": { "order_id": "ORD-1006" }
}
```

Response: `"message": "Payment buffered; awaiting order submission."`

Then submit the order normally. The workflow unparks, processes the buffered payment, and proceeds to `Fulfilled`.

---

# Fulfillment Output

After successful payment validation, the fulfillment integration receives a message matching the spec contract:

```json
{
  "customer_id": "CUST-001",
  "order_id": "ORD-1001",
  "payment_details": {
    "rrn": "RRN-100001"
  },
  "items": [
    {
      "item_id": "ITEM-001",
      "quantity": 2,
      "sku_id": "SKU-ITEM-001",
      "brand_code": "BRAND-001"
    }
  ]
}
```

Kafka is intentionally not used in this POC. Fulfillment is represented by a Temporal Activity / integration boundary.

---

# Temporal Web UI

```powershell
.\temporal-start.ps1
```

Temporal frontend: `localhost:7233`  
Temporal Web UI: `http://localhost:8233`

After creating an order, search for the workflow using the order ID (e.g. `ORD-1001`). The history shows activities, signals, timers, and state transitions. The `OrderStatus` keyword search attribute is upserted on every transition so you can filter: `OrderStatus = 'WaitingForPayment'`.

---

# Swagger UI

```text
http://localhost:5000/swagger
```

Replace `5000` with the port displayed by the OMS API.

Happy-path sequence:

```text
1. POST /api/orders
2. GET  /api/orders/{orderId}
3. POST /api/orders/{orderId}/payment
4. GET  /api/orders/{orderId}
```

Invalid-order sequence:

```text
1. POST /api/orders          (item_id contains "INVALID")
2. GET  /api/orders/{orderId}
3. POST /api/orders/{orderId}/support-correction
4. GET  /api/orders/{orderId}
5. POST /api/orders/{orderId}/payment
6. GET  /api/orders/{orderId}
```

---

# Start the POC

## Terminal 1 — Temporal

```powershell
.\temporal-start.ps1
```

Registers `OrderStatus=Keyword` and starts the dev server.

## Terminal 2 — OMS API + worker promotion

```powershell
.\run.ps1
```

Restores, starts the API, waits for the worker to poll, then promotes the deployment version.
