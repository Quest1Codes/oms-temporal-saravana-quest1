# OMS Temporal POC

A .NET 9 proof of concept for the Partner Application Assessment: Order Processing Management System (OMS).

## Scope

- Temporal is the durable orchestration layer.
- No Kafka is used in this POC.
- Temporal CLI dev server uses its default **in-memory persistence** for the Temporal service.
- Application order/dashboard data uses SQLite-backed storage via the repository layer.
- Commerce, PIM, Payment, and Fulfillment are mocked as in-process services behind Activities.
- ASP.NET Core hosts the API and the Temporal Worker in the same process for a simple demo.
- Temporal Web UI is provided by `temporal server start-dev` at `http://localhost:8233`.

## Assessment mapping

| Assessment requirement | POC implementation |
|---|---|
| Inputs can arrive in different sequences | Payment is delivered asynchronously as a Temporal Signal |
| Missing input | Workflow waits durably for a signal |
| 30-day TTL | Temporal timer via `Workflow.WaitConditionAsync(..., timeout)` |
| Expired order | Dashboard activity records `Expired` |
| Order validation | `ValidateOrderAsync` Activity |
| Support correction | `SupportCorrection` Signal, followed by re-validation |
| Enrichment | `EnrichOrderAsync` Activity |
| Payment capture | `PaymentCaptured` Signal + payment validation Activity |
| Cancellation | `CancelOrder` Signal before capture |
| Dashboard | In-memory repository Activity |
| Fulfillment | Mock fulfillment Activity |
| Failure handling | Activity retry policies and explicit business-state handling |
| vNext risk | Optional `RiskData` model field and documented extension point |
| PII | Email is intentionally not included in the initial workflow payload; README documents the future protection approach |

## Prerequisites

- .NET 9 SDK
- Temporal CLI

The Temporal .NET SDK currently has 1.18.0 available on NuGet. This project pins that version for repeatable builds.

## Run

Terminal 1:

```powershell
temporal server start-dev
```

The Temporal dev server defaults to in-memory persistence and starts the Web UI. The server is available on `localhost:7233` and the UI on `http://localhost:8233`.

Terminal 2:

```powershell
dotnet restore
dotnet run --project src/OMS.Api
```

Open the API shown by ASP.NET Core and the Temporal UI at `http://localhost:8233`.

## Demo

Create an order:

```http
POST /api/orders
Content-Type: application/json

{
  "customerId": "CUST-1001",
  "order": {
    "orderId": "ORD-1001",
    "items": [
      { "itemId": "ITEM-001", "quantity": 2 },
      { "itemId": "ITEM-002", "quantity": 1 }
    ]
  }
}
```

The workflow validates and enriches the order, then waits for payment.

Capture payment:

```http
POST /api/orders/ORD-1001/payment
Content-Type: application/json

{
  "customerId": "CUST-1001",
  "rrn": "RRN-1001",
  "amountCents": 12999
}
```

Query status:

```http
GET /api/orders/ORD-1001
```

Cancel before payment:

```http
POST /api/orders/ORD-1001/cancel
Content-Type: application/json

{ "reason": "Customer requested cancellation" }
```

For an invalid order, include an item ID containing `INVALID`, then send:

```http
POST /api/orders/ORD-1002/support-correction
Content-Type: application/json

{
  "items": [
    { "itemId": "ITEM-001", "quantity": 1 }
  ]
}
```

## Temporal UI

The UI is the primary demo surface for the assessment. It shows Workflow Execution history, Activities, Signals, timers, retries, and final status.

## Important POC limitation

The Temporal dev server's default in-memory persistence intentionally loses Workflow histories when the dev server stops. This is suitable for a POC/demo. If we later want restart durability, change the command to `temporal server start-dev --db-filename .temporal/temporal.db`.

## Temporal operational decisions

- Activity options use `StartToCloseTimeout` for an individual attempt and `ScheduleToCloseTimeout` to bound the full retry window.
- The 30-day payment lifetime is owned by the workflow timer. The API only delivers signals and does not enforce the lifetime.
- Workflow state is kept in one state object. Signals buffer intent and are reconciled by the workflow body, so payment can arrive before enrichment completes.
- Fulfillment submission is idempotent by order ID in the mock service. If recording the fulfilled state fails after submission, the workflow runs a retryable compensation activity and preserves the technical failure if compensation succeeds.
- There are no local activities because all current integrations represent service calls. Configuration used by workflow code must be passed in or loaded through an activity.
- The POC uses one task queue because there is no host-targeting or rate-limiting requirement. Worker concurrency should be tuned only after observing CPU, memory, activity latency, and queue metrics.
- Continue-as-new is intentionally not used: the workflow has one bounded payment wait and a small history. Add it when history size or workflow lifetime becomes material, and carry the state object forward.
- Before deploying incompatible workflow changes, introduce Temporal worker versioning/build IDs and keep old workers available until existing executions drain. The POC currently runs a single worker fleet without version routing.
- The in-memory repository and Temporal dev database are demonstration choices. Production deployment requires durable application storage, alerts for activity failures/compensation failures, and a documented worker rollout procedure.

## Production configuration and monitoring

The API defaults are in `src/OMS.Api/appsettings.json` and should be overridden with environment-specific configuration:

- `ConnectionStrings__Orders`: durable database connection string. The sample uses SQLite at `./data/orders.db`; use a managed, backed-up database for multiple API replicas.
- `Temporal__TargetHost` and `Temporal__Namespace`: Temporal endpoint and namespace.
- `Temporal__WorkerDeploymentName` and `Temporal__WorkerBuildId`: stable deployment name plus an immutable release identifier such as the container image digest or CI build number. Keep the previous build available while existing workflows drain.
- `Temporal__MaxConcurrentActivities` and `Temporal__MaxConcurrentWorkflowTasks`: starting limits. Tune from CPU, memory, activity latency, queue age, and downstream rate-limit metrics; do not treat these as task-queue rate limits.

Prometheus metrics are exposed at `/metrics`. The application exports ASP.NET/runtime metrics plus `oms_activity_executions_total` and `oms_activity_failures_total`, labeled by activity name. Starter Prometheus rules are in [deploy/prometheus-alerts.yml](deploy/prometheus-alerts.yml); production alerting should additionally cover workflow-task failure rate, payment-wait queue age, and worker/task-queue backlog. Alert thresholds should be set from a baseline under normal order volume rather than copied from the POC defaults.

## PII approach for vNext

The initial assessment payload does not require customer email. When email is introduced, avoid putting unnecessary PII into Workflow history. Prefer a reference/token to protected application storage, encrypt sensitive fields at rest, restrict access, redact logs, and use a custom Temporal payload codec/encryption strategy where appropriate.

## Risk vNext

Add risk collection as a new Activity or Child Workflow between validation and enrichment. Keep the workflow input extensible with an optional `RiskData` field so the first release can remain compatible.
