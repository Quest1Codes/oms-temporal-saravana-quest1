# OMS Temporal POC

A .NET 9 proof of concept for the Partner Application Assessment: Order Processing Management System (OMS).

## Scope

- Temporal is the durable orchestration layer.
- No Kafka is used in this POC.
- Temporal CLI dev server uses its default **in-memory persistence** for the Temporal service.
- Application order/dashboard data uses **SQLite-backed storage** via `SqliteOrderRepository`.
- Commerce, PIM, Payment, and Fulfillment are mocked as in-process services behind Activities.
- ASP.NET Core hosts the API and the Temporal Worker in the same process for a simple demo.
- Temporal Web UI is provided by `temporal server start-dev` at `http://localhost:8233`.

## Assessment mapping

| Assessment requirement | POC implementation |
|---|---|
| Inputs can arrive in different sequences | Payment is delivered asynchronously as a Temporal Signal; early payment is buffered in workflow state |
| Missing input | Workflow waits durably for a signal bounded by the 30-day TTL |
| 30-day TTL | `orderDeadline = Workflow.UtcNow + OrderTtl` captured at the top of `RunAsync`; all subsequent waits use the remaining time |
| Expired order | Dashboard activity records `Expired` |
| Order validation | `ValidateOrderAsync` Activity on dedicated `oms-commerce` task queue |
| Support correction | `CorrectOrder` Signal/Update, followed by re-validation within the remaining TTL window |
| Enrichment | `EnrichOrderAsync` Activity |
| Payment capture | `CapturePayment` Signal/Update + payment validation Activity; loops back on invalid RRN |
| Cancellation | `CancelOrder` Signal/Update; rejected after payment capture |
| Dashboard | SQLite upsert (`ON CONFLICT DO UPDATE`) Activity |
| Fulfillment | Mock fulfillment Activity (idempotent by order ID) |
| Failure handling | Differentiated retry policies per activity class; dashboard writes are eventually-consistent projections with unbounded retry |
| Commerce rate limit | Dedicated `oms-commerce` task queue with `MaxTaskQueueActivitiesPerSecond = 150` |
| vNext risk | Optional `RiskData` model field and documented extension point — see [Risk vNext](#risk-vnext) |
| PII | Email is intentionally not included in the initial workflow payload — see [PII approach for vNext](#pii-approach-for-vnext) |

## Prerequisites

- **.NET 9 SDK** (`global.json` pins `9.0.317`; install from https://dotnet.microsoft.com/download/dotnet/9.0)
- **Temporal CLI** (https://docs.temporal.io/cli)

The Temporal .NET SDK is pinned to `1.18.0` for repeatable builds.

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
  "customer_id": "CUST-1001",
  "order": {
    "order_id": "ORD-1001",
    "items": [
      { "item_id": "ITEM-001", "quantity": 2 },
      { "item_id": "ITEM-002", "quantity": 1 }
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
  "customer_id": "CUST-1001",
  "order_id": "ORD-1001",
  "rrn": "RRN-1001",
  "amount_cents": 12999
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
    { "item_id": "ITEM-001", "quantity": 1 }
  ]
}
```

## Temporal UI

The UI is the primary demo surface for the assessment. It shows Workflow Execution history, Activities, Signals, Updates, timers, retries, and final status. The `OrderStatus` keyword search attribute is upserted on every state transition so you can filter executions by status directly in the UI (`OrderStatus = 'WaitingForPayment'`).

## Important POC limitation

The Temporal dev server's default in-memory persistence intentionally loses Workflow histories when the dev server stops. This is suitable for a POC/demo. If you later want restart durability, change the command to `temporal server start-dev --db-filename .temporal/temporal.db`.

## Temporal operational decisions

- The 30-day payment lifetime is anchored to `Workflow.UtcNow` at the very start of `RunAsync`. Every subsequent wait — correction, payment, and payment-retry — uses the remaining time against this single deadline, so an order can never be forwarded more than 30 days after submission regardless of how many correction rounds it takes.
- Activity options are differentiated by class:
  - **Commerce validation** (`oms-commerce` queue): `StartToClose = 15 s`, `ScheduleToClose = 2 min`. Bounded tightly because the dedicated queue already absorbs rate-limit pressure.
  - **PIM enrichment / Fulfillment**: `StartToClose = 30 s`, `ScheduleToClose = 4 h`, `MaximumInterval = 5 min`. Hours window prevents a brief downstream outage from permanently failing a 30-day order.
  - **Dashboard writes** (status and fulfilled): `StartToClose = 15 s`, `ScheduleToClose = null` (unbounded), `MaximumInterval = 5 min`. Dashboard rows are eventually-consistent projections; a DB blip must never fail the business transaction.
  - **Payment validation**: `StartToClose = 10 s`, `ScheduleToClose = 1 min`. A tight budget is intentional — a 1-minute outage parks the order back at `WaitingForPayment` for a fresh capture rather than burning retries.
- Workflow state is kept in one private `WorkflowState` object. Signals/Updates buffer intent and are reconciled by the workflow body, so payment can arrive before enrichment completes.
- Commerce validation runs on the dedicated `oms-commerce` task queue with `MaxTaskQueueActivitiesPerSecond = 150` to honour the documented 150 RPS rate limit. Other activities run on the default `oms-order-processing` queue.
- Signal handlers use Update validators to reject inputs in the wrong phase (cancel after capture, correction while not in `ValidationFailed`). All three controllers map Update failures to HTTP 409 Conflict so callers receive synchronous feedback.
- Cancellation is gated at two levels: the `CancelAsync` signal handler returns early if `state.PaymentCapture != null`, and the `CancelOrderUpdateAsync` validator throws. A cancel that arrives after capture is silently dropped by the signal and rejected with a 409 by the Update.
- Payment validation failure clears `state.PaymentCapture` and loops back to the payment wait with the remaining TTL — `PaymentRejected` is no longer a terminal dead-end.
- Fulfillment submission is idempotent by order ID in the mock service. A `SaveFulfilledAsync` failure after fulfillment acceptance is treated as a transient projection failure and does not roll back the business outcome; the workflow returns `Fulfilled` with a note in the message.
- Continue-as-new is intentionally not used: the workflow has one bounded payment wait and a small history. Add it when history size or workflow lifetime becomes material, and carry the state object forward.
- Worker versioning (`DeploymentOptions`) is configured with `DefaultVersioningBehavior = Pinned` and `[Workflow(VersioningBehavior = Pinned)]`. Before deploying incompatible workflow changes, promote the new deployment version with `temporal worker deployment set-current-version --deployment-name oms-order-worker --build-id <new-id>` and keep old workers running until existing executions drain (see [Versioning vNext](#versioning-and-worker-deployment-vnext)).
- The SQLite repository and Temporal dev database are demonstration choices. Production deployment requires durable application storage, alerts for activity failures, and a documented worker rollout procedure.

## Production configuration and monitoring

The API defaults are in `src/OMS.Api/appsettings.json` and should be overridden with environment-specific configuration:

- `ConnectionStrings__Orders`: durable database connection string. The sample uses SQLite at `./data/orders.db`; use a managed, backed-up database for multiple API replicas.
- `Temporal__TargetHost` and `Temporal__Namespace`: Temporal endpoint and namespace.
- `Temporal__WorkerDeploymentName` and `Temporal__WorkerBuildId`: stable deployment name plus an immutable release identifier such as the container image digest or CI build number. Keep the previous build available while existing workflows drain (see [Versioning vNext](#versioning-and-worker-deployment-vnext)).
- `Temporal__MaxConcurrentActivities` and `Temporal__MaxConcurrentWorkflowTasks`: starting limits. Tune from CPU, memory, activity latency, queue age, and downstream rate-limit metrics; do not treat these as task-queue rate limits.

Prometheus metrics are exposed at `/metrics`. The application exports ASP.NET/runtime metrics plus `oms_activity_executions_total` and `oms_activity_failures_total`, labeled by activity name. Starter Prometheus rules are in [deploy/prometheus-alerts.yml](deploy/prometheus-alerts.yml); production alerting should additionally cover workflow-task failure rate, payment-wait queue age, and worker/task-queue backlog. Alert thresholds should be set from a baseline under normal order volume rather than copied from the POC defaults.

The `/health` endpoint performs a live `GetSystemInfoAsync` call against the Temporal server and returns HTTP 503 with a `degraded` status when Temporal is unreachable, so load-balancer health checks can distinguish "API running but Temporal down" from "API not running".

## PII approach for vNext

The initial assessment payload does not require customer email. When email is introduced:

1. **Minimise history exposure.** Store the email in protected application storage (e.g. a KMS-encrypted column) and pass only a short-lived reference token into the workflow. The token appears in Workflow history; the raw email does not.

2. **Payload codec.** Implement `IPayloadCodec` (Temporal .NET SDK) to encrypt workflow payloads with AES-256-GCM. Manage keys via AWS KMS or Azure Key Vault; embed a `keyId` in the payload metadata so rotation is possible without re-encryption of existing history. Deploy a [Temporal Codec Server](https://docs.temporal.io/security#codec-server) so the Temporal Web UI and CLI can decrypt payloads for authorised operators without the SDK.

3. **Search attributes.** Never put PII into search attributes — Temporal does not apply the codec to them. `OrderStatus` and `OrderDeadline` are safe; `CustomerEmail` is not.

4. **Log redaction.** Add a structured-logging enricher that replaces email-shaped strings with `[REDACTED]` before any log sink. Enforce via a linting rule or code review checklist.

5. **Access control.** Restrict Temporal namespace access to the application service account. Scope the codec server behind an internal auth proxy so only on-call engineers with an active session can decrypt.

6. **Self-hosted vs Temporal Cloud.** On Temporal Cloud, the platform does not read payload data; the codec-server pattern is still the right choice for defence in depth. On self-hosted, encrypt the Temporal persistence layer in addition to applying the codec.

## Risk vNext

Add risk collection as a new Activity between validation and enrichment. Keep the workflow input extensible with the existing optional `RiskData` field (already present in `OrderModels.cs`) so the first release stays backward-compatible.

If risk data arrives asynchronously (e.g. a separate webhook), add a `ReceiveRiskData` signal or update with its own wait bounded by the remaining TTL deadline, placed after validation and before enrichment. Document whether risk input is required to proceed or advisory only.

Child workflows are not justified here — risk collection is a single activity call with the same lifecycle as the parent order. A child workflow would be appropriate only if risk had its own long wait (e.g. manual review) that needs independent retry and history management.

## Versioning and worker deployment vNext

The POC uses Pinned worker versioning. Here is the full deploy → drain → retire sequence for a breaking workflow change:

### Deploy

1. Bump `Temporal__WorkerBuildId` in the deployment manifest (use an immutable identifier such as the container image digest).
2. Deploy the new worker alongside the old one (both run simultaneously).
3. Register the new version with Temporal:
   ```powershell
   temporal worker deployment set-current-version \
     --deployment-name oms-order-worker \
     --build-id <new-build-id>
   ```
   New workflow executions are automatically pinned to the new build. Existing executions continue on the old build.

### Drain check

4. Poll until all executions on the old build have reached a terminal state:
   ```powershell
   temporal workflow list \
     --query "ExecutionStatus='Running' AND TemporalWorkerBuildId='<old-build-id>'"
   ```
   Automate this in CI/CD; block the next step until the list is empty.

### Deprecate patch

5. If the change used `Workflow.Patched("feature-flag")`, deprecate the patch once the old build is drained:
   ```csharp
   // Replace Workflow.Patched("feature-flag") with Workflow.DeprecatePatch("feature-flag")
   // in the first build after drain, then remove both calls in the following build.
   ```
   Removing a patch before draining causes replay failures. Always follow the three-build lifecycle: introduce → deprecate → remove.

### Remove old worker

6. Once drain is confirmed and patches are deprecated, stop and remove the old worker deployment.

### Replay gating in CI

Commit a JSON history snapshot for each terminal workflow path (Fulfilled, Expired, Cancelled, FulfillmentFailed) using `WorkflowEnvironment.GetResult` with `exportHistory: true`. Run `WorkflowReplayer.ReplayWorkflowsAsync` against those fixtures in CI to catch non-determinism before merging any workflow code change.
