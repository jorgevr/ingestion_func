# Event Type Manifest

Registered event types for the energy-ingestion-boundary service.
All event types MUST be registered here before first use per constitution Principle VII.

> **Service Bus tier**: Basic — queues only. No topics or subscriptions.
> Queue names are listed in the "Queue" column below.

| Event Type | Queue | Owner | Schema | Description |
| --- | --- | --- | --- | --- |
| `solar.pvdaq.dataset.available` | `raw-energy-events` | `historical_worker` | `dataset-event.json` | Dataset-level notification: a CSV file has been stored in ADLS Gen2 bronze layer |

## Retired Event Types

The following types were registered before the spec migrated to the dataset-level pattern
(session 2026-03-09). They are no longer emitted by this service. Per constitution §VII,
`raw.*` event types are reserved for downstream processing layers.

| Event Type | Retired | Notes |
| --- | --- | --- |
| `raw.pvdaq.generation.v1` | 2026-03-09 | Superseded by `solar.pvdaq.dataset.available` |
| `raw.pvdaq.historical.v1` | 2026-03-09 | Superseded by `solar.pvdaq.dataset.available` |
