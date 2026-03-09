# Feature Specification: Event Routing (Event Grid to Service Bus)

**Feature Branch**: `003-event-routing`
**Created**: 2026-03-08
**Status**: Draft
**Input**: User description: "Event Routing — receives events when PVDAQ historical data completes ingestion, filters by entity type and tenant, routes to tenant-scoped Service Bus queues with fan-out and dead-letter support."

## Context

This is **Layer 2** in the PVDAQ data pipeline. Layer 1 (features 001 and 002) ingests CSV data from the OEDI Data Lake, validates records, and writes them as CloudEvents to a Service Bus topic. It also tracks file processing status in the `PvdaqFileTracking` Azure Table.

Layer 2 receives completion events from Layer 1 and routes them to the correct downstream queues based on entity type and tenant. It does **not** parse, transform, or process data — it routes only.

```text
Layer 1 (Ingestion) ──► Blob Storage (CSV) + PvdaqFileTracking (metadata)
                              │
                         BlobCreated event (Event Grid system topic)
                              │
                              ▼
Layer 2 (THIS SPEC) ──► Correlate with tracking table ──► Filter + Fan-out ──► Service Bus queues
                              │
                              ▼
Layer 3 (Processing) ──► future
```

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Route Completed File Events to Tenant Queues (Priority: P1)

When Layer 1 writes a CSV file to blob storage and marks it as "completed" in `PvdaqFileTracking`, the routing layer receives the `BlobCreated` event, correlates it with the tracking table, and delivers a message to the correct tenant-scoped Service Bus queue so that downstream processors know a new file is ready.

**Why this priority**: This is the core routing capability. Without it, downstream systems have no way to know when data is ready for processing.

**Independent Test**: Can be fully tested by uploading a blob and creating a "completed" tracking entity, then verifying a message appears on the expected tenant queue within the delivery SLA.

**Acceptance Scenarios**:

1. **Given** a blob is created and the corresponding tracking entity has status "completed" for site 1234, **When** Event Grid delivers the `BlobCreated` event, **Then** a routing message is placed on the queue `default.solar-site.ingest` within 30 seconds.
2. **Given** a `BlobCreated` event is received, **When** the corresponding tracking entity contains a valid site ID and entity type, **Then** the routed message includes the original site ID, file name, entity type, and a correlation ID.
3. **Given** a `BlobCreated` event is received but the corresponding tracking entity has status "failed" or "processing", **When** the routing layer looks up the entity, **Then** no routing message is emitted (only "completed" entries trigger routing).

---

### User Story 2 - Filter Events by Entity Type (Priority: P1)

The routing layer filters incoming events by entity type (solar site, meter, inverter) and directs each event to the appropriate entity-type-specific queue, so downstream processors only receive events relevant to their domain.

**Why this priority**: Entity-type filtering is essential to ensure each processor receives only the events it can handle, preventing misrouted data.

**Independent Test**: Can be tested by emitting events with different entity types and verifying each lands on the correct queue.

**Acceptance Scenarios**:

1. **Given** an event with entity type "solar-site", **When** the routing layer processes it, **Then** the message is placed on `default.solar-site.ingest`.
2. **Given** an event with entity type "meter", **When** the routing layer processes it, **Then** the message is placed on `default.meter.ingest`.
3. **Given** an event with an unrecognized entity type, **When** the routing layer processes it, **Then** the event is routed to the tenant dead-letter queue with an "unknown_entity_type" reason.

---

### User Story 3 - Fan-Out to Multiple Queues (Priority: P2)

When a single event needs to reach multiple downstream processors (e.g., a solar-site completion event should also notify an analytics queue), the routing layer fans out by delivering copies to all configured destination queues.

**Why this priority**: Fan-out extends the routing layer's utility but is not required for the initial single-consumer case.

**Independent Test**: Can be tested by configuring a fan-out rule for "solar-site" to two queues and verifying both receive the message.

**Acceptance Scenarios**:

1. **Given** a fan-out rule maps "solar-site" to queues `[default.solar-site.ingest, default.analytics.ingest]`, **When** a solar-site event arrives, **Then** both queues receive a copy of the routing message.
2. **Given** a fan-out rule maps "meter" to a single queue, **When** a meter event arrives, **Then** only that one queue receives the message (no unnecessary duplication).

---

### User Story 4 - Dead-Letter Unroutable Events (Priority: P2)

Events that cannot be routed (missing metadata, unknown entity type, delivery failure) are sent to a tenant-scoped dead-letter queue with diagnostic information, so operators can investigate and reprocess them.

**Why this priority**: Dead-lettering ensures no event is silently lost, which is critical for at-least-once delivery guarantees.

**Independent Test**: Can be tested by sending an event with missing tenant ID and verifying it appears on the dead-letter queue with error details.

**Acceptance Scenarios**:

1. **Given** an event with a missing or null tenant ID, **When** the routing layer processes it, **Then** the event is sent to a global dead-letter queue with reason "missing_tenant_id".
2. **Given** an event that fails to deliver to the target queue after retries, **When** delivery is exhausted, **Then** the event is dead-lettered with reason "delivery_failed" and includes the original event payload.

---

### User Story 5 - Infrastructure as Code (Priority: P3)

All Azure resources required by the routing layer (Event Grid topic, subscriptions, Service Bus queues) are defined in Bicep templates, so infrastructure can be provisioned and updated consistently across environments.

**Why this priority**: IaC is important for production readiness but does not block functional development or testing.

**Independent Test**: Can be tested by deploying the Bicep template to a test resource group and verifying all resources are created with correct configurations.

**Acceptance Scenarios**:

1. **Given** the Bicep template is deployed to a new resource group, **When** deployment completes, **Then** the Event Grid topic, subscriptions with filters, and all Service Bus queues are created.
2. **Given** a new tenant is added to the configuration, **When** the Bicep template is redeployed, **Then** the new tenant's queues are provisioned without affecting existing tenants.

---

### Edge Cases

- What happens when the same event is delivered twice by Event Grid (at-least-once)? The routing layer should be idempotent — delivering duplicate messages to the queue is acceptable since downstream processors already handle idempotency.
- What happens when a tenant has no configured queues? The event is dead-lettered with reason "no_routes_configured".
- What happens when the Service Bus namespace is temporarily unavailable? Event Grid retries delivery with exponential backoff per its built-in retry policy (up to 24 hours).
- What happens when the blob has no corresponding `PvdaqFileTracking` entity? The event is dead-lettered with reason "no_tracking_entity".
- What happens when the tracking entity has no entity type metadata? The event is dead-lettered with reason "missing_entity_type".

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST subscribe to `Microsoft.Storage.BlobCreated` events via an Event Grid system topic on the Storage account, correlate each event with the `PvdaqFileTracking` table, and process only entries with status "completed".
- **FR-002**: System MUST extract entity type and tenant ID from the event payload to determine routing destinations.
- **FR-003**: System MUST route events to tenant-scoped Service Bus queues using the naming convention `{tenant}.{entity_type}.ingest`.
- **FR-004**: System MUST support fan-out routing where a single event can be delivered to multiple destination queues based on configurable rules.
- **FR-005**: System MUST dead-letter events that cannot be routed (missing metadata, unknown entity type, delivery failure) to a `{tenant}.deadletter` queue (or a global dead-letter queue if tenant is unknown).
- **FR-006**: System MUST include diagnostic metadata in dead-letter messages: original event payload, failure reason, timestamp, and correlation ID.
- **FR-007**: System MUST guarantee at-least-once delivery — no event may be silently dropped.
- **FR-008**: System MUST support the following entity types: `solar-site`, `meter`, `inverter`. This is the complete list.
- **FR-009**: System MUST use Bicep templates to define all infrastructure (Event Grid topic, subscriptions, Service Bus queues).
- **FR-010**: System MUST use Service Bus Standard SKU to support topics and subscriptions.
- **FR-011**: Routing messages MUST include: source event ID, site ID, entity type, tenant ID, file name, correlation ID, and timestamp.

### Key Entities

- **Routing Event**: A `Microsoft.Storage.BlobCreated` event received via an Event Grid system topic. Contains the blob URL and storage account metadata. The routing layer correlates this with the `PvdaqFileTracking` table to obtain site ID, entity type, and file status.
- **Routing Message**: The Service Bus message delivered to a tenant queue. Includes site ID, entity type, tenant ID, file reference, and correlation ID. Does not include the actual data — only metadata for downstream processors to fetch what they need.
- **Routing Rule**: A configuration entry that maps an entity type (or entity type + tenant combination) to one or more destination queues. Supports fan-out.
- **Dead-Letter Entry**: A message placed on a dead-letter queue containing the original event, failure reason, and diagnostic metadata.

## Assumptions

- **Trigger source**: The routing layer uses an Event Grid system topic on the Storage account to receive blob events (`Microsoft.Storage.BlobCreated`). When Layer 1 writes a CSV file to blob storage, the system topic fires an event. The routing layer then correlates with the `PvdaqFileTracking` table to extract enriched metadata (site ID, entity type, file status). Only blobs whose corresponding tracking entity has status "completed" trigger downstream routing.
- **Tenant identification**: All PVDAQ sites initially belong to a single default tenant (`default`). All queues use the naming convention `default.{entity_type}.ingest`. Multi-tenant support can be introduced later by adding a tenant-to-site mapping — no architectural changes required, only configuration and IaC updates.
- **Service Bus namespace**: The routing layer reuses the same Service Bus namespace already provisioned for features 001/002.
- **Identity-based auth**: The routing layer uses `DefaultAzureCredential` (RBAC) consistent with features 001 and 002 — no connection strings or SAS tokens.
- **Queue auto-creation**: Queues for new tenants/entity types are created via Bicep deployment, not dynamically at runtime.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Events from completed file ingestions are routed to the correct tenant queue within 30 seconds of the tracking table update, under normal operating conditions.
- **SC-002**: 100% of events are either successfully routed or dead-lettered — zero silent drops.
- **SC-003**: Fan-out events are delivered to all configured destination queues for a given entity type.
- **SC-004**: Dead-lettered events contain sufficient diagnostic information for an operator to identify the failure cause and reprocess manually.
- **SC-005**: All routing infrastructure can be provisioned from scratch in a new environment using a single Bicep deployment.
- **SC-006**: Adding a new tenant or entity type requires only configuration/IaC changes — no code modifications.
