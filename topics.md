# Event Type Manifest

Registered event types for the energy-ingestion-boundary service.
All event types MUST be registered here before first use per constitution Principle VII.

| Event Type | Topic | Owner | Schema | Description |
| --- | --- | --- | --- | --- |
| `raw.pvdaq.generation.v1` | `raw-energy-events` | `pvdaq_ingest` | `pvdaq-v1.json` | Raw PVDAQ photovoltaic telemetry data |
| `raw.pvdaq.historical.v1` | `raw-energy-events` | `historical_worker` | `pvdaq-v1.json` | Historical PVDAQ telemetry from OEDI 2023-solar-data-prize |
| `solar.pvdaq.dataset.available` | `raw-energy-events` | `historical_worker` | `dataset-event.json` | Dataset-level notification: a CSV file has been stored in ADLS Gen2 |
