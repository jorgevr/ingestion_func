# Comprehensive Requirements Quality Checklist: PVDAQ Ingestion

**Purpose**: Full requirements quality audit — completeness, clarity, consistency, measurability, and coverage across all spec sections.
**Created**: 2026-02-20
**Feature**: [spec.md](../spec.md) | [plan.md](../plan.md) | [data-model.md](../data-model.md)
**Depth**: Standard (~30 items) | **Audience**: PR Reviewer | **Focus**: All quality dimensions

## Requirement Completeness

- [ ] CHK001 — Is an explicit FR defined for idempotency TTL cleanup (daily purge of expired records)? The data model describes the cleanup mechanism but no FR mandates it. [Gap]
- [ ] CHK002 — Are `host.json` concurrency and scaling configuration requirements specified? The constitution mandates per-vendor tuning of `functionTimeout`, `maxPollingInterval`, and scaling limits, but no FR addresses this. [Gap, Constitution §Dev Workflow]
- [ ] CHK003 — Is the creation of `schemas/README.md` (schema discovery mechanism and naming convention) required by an FR? Constitution Principle II mandates it. [Gap, Constitution §II]
- [ ] CHK004 — Is the creation or registration of `topics.md` manifest required before first emission? FR-007 references it as a prerequisite but no FR owns its creation. [Gap, Spec §FR-007]
- [ ] CHK005 — Are alerting requirements specified? Constitution Principle V mandates configurable alerts on validation failure spikes, emission failures, and abnormal latency, but no FR addresses alert definitions. [Gap, Constitution §V]
- [ ] CHK006 — Is a requirement defined for the fail-closed behavior when the idempotency store is unavailable? Edge Case 3 describes it but no FR explicitly mandates it. [Gap, Spec §Edge Cases]

## Requirement Clarity

- [ ] CHK007 — Is "exponential backoff" quantified with base delay, max delay, and jitter parameters for PVDAQ API retries? FR-003 specifies "max 3 attempts" but no timing parameters. [Ambiguity, Spec §FR-003]
- [ ] CHK008 — Is the lookback time window unit explicitly stated in FR-002? The phrase "configurable lookback time window" does not specify hours/minutes/days. The data model says hours, but the FR itself is ambiguous. [Clarity, Spec §FR-002]
- [ ] CHK009 — Is SC-001's "within 60 seconds" scoped to per-record, per-site, or per-invocation? The exclusion of "upstream API latency" makes the remaining measurement boundary unclear. [Ambiguity, Spec §SC-001]
- [ ] CHK010 — Is the idempotency composite key format explicitly documented in the FR? FR-008 says "site ID + timestamp" but does not specify the string format (e.g., `{SiteID}_{measdatetime_iso}`). The data model defines it, but the FR does not cross-reference. [Clarity, Spec §FR-008]
- [ ] CHK011 — Is the dead-letter "error metadata" content specified in FR-005 or cross-referenced to the data model? FR-005 says "with error metadata" without enumerating the fields. [Clarity, Spec §FR-005]
- [ ] CHK012 — Does FR-012's "partial batch failure resilience" clearly define the granularity — is it record-level within a site, site-level within an invocation, or both? [Ambiguity, Spec §FR-012]

## Requirement Consistency

- [ ] CHK013 — Is the Assumption that `pvdaq_access` is an external library (§Assumptions, line 298–300) reconciled with the research finding that no such PyPI package exists? The assumption states it "provides a stable interface" as though pre-existing. [Conflict, Spec §Assumptions, research.md §R1]
- [ ] CHK014 — Is SC-003's "24-hour window" for deduplication consistent with the idempotency TTL being a "minimum 24 hours"? An exact-24-hour TTL could allow a re-emission at the boundary due to timing. [Conflict, Spec §SC-003 vs §Key Entities]
- [ ] CHK015 — Does FR-010's wording "no secrets in...environment variables" conflict with the Key Vault reference mechanism, which resolves secrets INTO environment variables at runtime? The intent is clear, but the literal wording could confuse implementers. [Ambiguity, Spec §FR-010]
- [ ] CHK016 — Are the exponential backoff parameters consistent between PVDAQ API retries (FR-003) and Service Bus emission retries (US3-AS2)? Both specify the pattern but neither defines shared or distinct parameters. [Consistency, Spec §FR-003 vs §US3]

## Acceptance Criteria Quality

- [ ] CHK017 — Can SC-001's 60-second target be objectively measured given the exclusion of "upstream API latency"? Is the measurement point defined (from trigger fire to last Service Bus send)? [Measurability, Spec §SC-001]
- [ ] CHK018 — Is SC-004's "complete set of structured telemetry metrics" enumerated, or does it rely on the reader cross-referencing FR-009? The success criterion should be self-contained or explicitly cross-reference. [Measurability, Spec §SC-004]
- [ ] CHK019 — Is SC-007's "single vendor API failure" tested at both the site-level (one site fails, others succeed) and the record-level (one record fails, rest of batch succeeds)? The criterion only addresses the site-level. [Coverage, Spec §SC-007]

## Scenario Coverage

- [ ] CHK020 — Are recovery requirements defined for idempotency records left in "pending" status after a crash between write and emit? The data model describes the pattern but no acceptance scenario validates it. [Gap, data-model.md §Idempotency Record]
- [ ] CHK021 — Is a scenario defined for function timeout during a long-running batch? What happens to records already emitted vs. records not yet processed? [Gap]
- [ ] CHK022 — Is a scenario defined for Service Bus topic throttling (HTTP 429 from Service Bus)? Edge Case 6 covers a missing topic but not a throttled one. [Gap, Spec §Edge Cases]
- [ ] CHK023 — Is a scenario defined for the validation schema file (`pvdaq-v1.json`) being missing or unreadable at runtime? [Gap]
- [ ] CHK024 — Is a scenario defined for PVDAQ API response exceeding the Service Bus message size limit (256 KB)? A single telemetry record is unlikely to exceed this, but the requirement doesn't address it. [Gap]

## Edge Case Coverage

- [ ] CHK025 — Are requirements defined for clock skew between the function host and the idempotency store affecting TTL calculations or lookback window accuracy? [Gap]
- [ ] CHK026 — Is behavior specified when `PVDAQ_SITE_IDS` is empty or contains an invalid (non-integer) value? [Gap, Spec §FR-011]
- [ ] CHK027 — Is behavior specified when the PVDAQ API returns records with a `measdatetime` outside the configured lookback window (stale data returned by the API)? [Gap]

## Non-Functional Requirements

- [ ] CHK028 — Are log volume or sampling requirements specified to control observability costs in production? Constitution Principle V defines structured logging but no cost-aware limits. [Gap, Constitution §V]
- [ ] CHK029 — Is a maximum per-invocation processing time or memory budget defined? The constraint section references "execution timeout" and "memory limits" but defers to `host.json` without specifying target values. [Clarity, Spec §Constraints]
- [ ] CHK030 — Is the PII constraint (§Constraints, "no raw payload content containing PII may appear in log output") specific enough? Does it define what fields constitute PII in the PVDAQ domain, or how to identify/redact them? [Clarity, Spec §Constraints]

## Dependencies & Assumptions

- [ ] CHK031 — Is the NREL PVDAQ API's availability SLA documented? The spec assumes availability but doesn't state expected uptime or planned maintenance windows. [Assumption, Spec §Assumptions]
- [ ] CHK032 — Is the Azure Table Storage entity size limit (1 MiB) addressed in relation to idempotency records? While unlikely to be exceeded, the constraint is not documented. [Assumption]
- [ ] CHK033 — Is the PVDAQ API's rate limit (stated as ~1,000 requests/hour in research) documented as a formal constraint, or only implied by the sequential polling assumption? [Assumption, Spec §Assumptions vs research.md §R1]

## Notes

- Check items off as completed: `[x]`
- Items referencing `[Gap]` indicate missing requirements that should be considered for addition
- Items referencing `[Ambiguity]` or `[Conflict]` indicate existing text that may need revision
- Items referencing `[Assumption]` indicate undocumented dependencies that should be validated
- Cross-reference with [constitution.md](../../../.specify/memory/constitution.md) for principle compliance
