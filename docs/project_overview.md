# Flight Project — OpenSky Real-Time Aviation Data Platform on Databricks

## 1. Project Definition

### 1.1 Objective

This project is a hands-on engineering sandbox in an aviation context. Its goal is to build a production-grade, governed, streaming data platform on Databricks that ingests **real-time aircraft positional data** from the [OpenSky Network](https://opensky-network.org/) public REST API, processes it through a medallion architecture, and exposes curated **data products** for analytics, operational consumption, and AI/ML enablement.

> OpenSky Network endpoint: `https://opensky-network.org/api/states/all`

### 1.2 Scope

**In scope**
- Infrastructure as Code (Terraform) for a Databricks workspace on GCP (free / community / trial tier).
- Unity Catalog governance model: catalogs, schemas, storage credentials, external locations, grants, tags.
- A **Structured Streaming** ingestion job that continuously polls the OpenSky REST endpoint and writes raw state vectors to a Delta Lake bronze table.
- A **Delta Live Tables (DLT)** pipeline implementing the medallion architecture (Bronze → Silver → Gold) with declarative data-quality expectations.
- Data-quality checks at every layer (schema, nullability, range, deduplication, freshness).
- A Databricks **Workflow / Job** that orchestrates streaming ingestion, the DLT pipeline, and downstream refresh — **no notebooks** are used for transformation logic; all logic lives in versioned Python/SQL source files executed by jobs.
- Exposed **data products** (e.g., *Current Flights*, *Airport Congestion*, *Anomaly Feed*) in the Gold layer with metadata and lineage visible in Unity Catalog.
- Lightweight Power BI / SQL-warehouse consumption guidance (semantic model stub).
- CI checks (SQL linter, `terraform fmt`, `terraform validate`, `ruff`/`pytest`).

**Out of scope (for this sandbox)**
- Cross-cloud replication / disaster recovery.
- Production-grade authentication against OpenSky (uses anonymous rate-limited access; OAuth2 client-credentials path documented but optional).
- Enterprise SSO / SCIM provisioning.
- Full MLOps training pipelines (only a feature-table stub is produced).

### 1.3 OpenSky Network — Source Data

The OpenSky Network `states/all` endpoint returns the current state vectors of all tracked aircraft worldwide. A single response contains:

```
{
  "time": 1753097600,            // unix timestamp of response
  "states": [
    [
      "3c6444",                  // icao24 (unique transponder address)
      "DLH123  ",                // callsign
      "Germany",                 // origin country
      1753097598.123,            // time_position
      6.1543,                    // last_contact (unix)
      50.0371,                   // longitude
      7.1234,                    // latitude
      11277.6,                   // baro_altitude (m)
      false,                     // on_ground
      232.5,                     // velocity (m/s)
      90.1,                      // true_track (deg)
      5.2,                       // vertical_rate (m/s)
      null,                      // sensors
      11450.6,                   // geo_altitude (m)
      "1000",                    // squawk
      false,                     // spi
      0                          // position_source
    ],
    ...
  ]
}
```

**Characteristics relevant to design**
- It is a **poll-based REST snapshot**, not a push stream → we wrap it with Structured Streaming's `rate` + `foreachBatch` / custom micro-batch source pattern.
- Rate-limited for anonymous clients (~hundreds of requests/day; 10s interval is safe).
- Highly denormalized array-of-arrays → must be flattened and typed on ingestion.
- Good vehicle for demonstrating late-data handling, watermarking, and deduplication by `(icao24, time_position)`.

### 1.4 Key Non-Functional Requirements

| NFR | Target |
|---|---|
| Availability | Best-effort (sandbox); jobs restartable, exactly-once via Delta `MERGE` |
| Freshness (Gold) | ≤ 15 minutes from source snapshot |
| Throughput | ~10–50 state-vector rows/sec (OpenSky global) |
| Governance | 100% of tables registered in Unity Catalog with owner, tags, and lineage |
| Reproducibility | Full provision via `terraform apply`; destroy via `terraform destroy` |
| Cost | Stays within Databricks free / community / GCP trial credits |

---

## 2. Architecture

### 2.1 High-Level Architecture

```
                         ┌────────────────────────────────────────────┐
                         │            OpenSky Network REST             │
                         │   https://opensky-network.org/api/states/all │
                         └───────────────────────┬────────────────────┘
                                                 │ HTTPS GET (polled)
                                                 ▼
 ┌──────────────────────────────────────────────────────────────────────────┐
 │                       Databricks Workspace (GCP)                          │
 │                                                                          │
 │  ┌─────────────────────┐   ┌──────────────────────────────────────────┐ │
 │  │ Structured Streaming │   │            Unity Catalog                 │ │
 │  │  Ingestion Job       │   │  catalog:  flight_cat                    │ │
 │  │  (PySpark, rate src) │──▶│  schemas:  bronze / silver / gold /     │ │
 │  │  foreachBatch write  │   │             governance / ml_features     │ │
 │  └─────────────────────┘   │  grants, tags, lineage, expectations     │ │
 │            │ Delta          └──────────────────────────────────────────┘ │
 │            ▼                                                              │
 │  ┌────────────────────────────────────────────────────────────────────┐ │
 │  │                     Delta Live Tables Pipeline                     │ │
 │  │              (Python/SQL source files, no notebooks)              │ │
 │  │                                                                    │ │
 │  │  BRONZE  (raw_states)        ── append, schema-on-read, quarantine │ │
 │  │     │  expect-valid, drop_or_quarantine                           │ │
 │  │     ▼                                                             │ │
 │  │  SILVER  (clean_states)      ── typed, deduped, watermark(late)   │ │
 │  │     │  expect-not-null, expect-range                              │ │
 │  │     ▼                                                             │ │
 │  │  GOLD    (data products)                                          │ │
 │  │     ├─ current_flights       (latest vector per icao24)           │ │
 │  │     ├─ airport_congestion     (aggregations near major airports)   │ │
 │  │     └─ anomaly_feed           (altitude/velocity outliers)         │ │
 │  └────────────────────────────────────────────────────────────────────┘ │
 │            │                                                              │
 │            ▼                                                              │
 │  ┌────────────────────┐  ┌────────────────────┐  ┌────────────────────┐  │
 │  │ Databricks Workflow│  │  SQL Warehouse /   │  │  MLflow Feature    │  │
 │  │  (orchestration)   │  │  Power BI semantic │  │  Store (optional) │  │
 │  └────────────────────┘  └────────────────────┘  └────────────────────┘  │
 └──────────────────────────────────────────────────────────────────────────┘
                                                 │
                                                 ▼
                                   GCS bucket (Delta storage root)
```

### 2.2 Layered (Medallion) Data Model

| Layer | Object | Format | Update Mode | Purpose |
|---|---|---|---|---|
| Bronze | `bronze.raw_states` | Delta | Append (streaming) | Immutable raw snapshots, schema-on-read, full fidelity incl. quarantine |
| Silver | `silver.clean_states` | Delta | `MERGE` (upsert by `icao24`, `time_position`) | Typed, deduplicated, watermarked, geo-enriched state vectors |
| Gold | `gold.current_flights` | Delta | `MERGE` (latest per `icao24`) | "Where is every aircraft right now" data product |
| Gold | `gold.airport_congestion` | Delta | Append (windowed agg) | Rolling counts / altitudes within airport proximity polygons |
| Gold | `gold.anomaly_feed` | Delta | Append | Outlier state vectors (altitude/velocity) for ops alerting & ML |
| Governance | `governance.dq_results` | Delta | Append | Data-quality metrics emitted by DLT expectations |
| ML | `ml_features.flight_features` | Delta | `MERGE` | Feature table stub for predictive maintenance / delay models |

### 2.3 Streaming Design

OpenSky exposes a **request/response snapshot**, so we model it as a streaming source using a **micro-batch polling pattern** rather than a native push stream:

1. A `rate` source (1 row / 10 s) drives the micro-batch cadence.
2. Each micro-batch executes an HTTP `GET` against `states/all` inside `foreachBatch`.
3. The JSON `states` array is flattened into typed rows and appended to `bronze.raw_states` with ingestion metadata (`ingest_ts`, `source_time`, `batch_id`).
4. Exactly-once is guaranteed by:
   - Idempotent `MERGE` on Silver keyed by `(icao24, time_position)`.
   - DLT checkpointing and Delta transactional writes.
5. **Watermark** on `time_position` (10 min) discards late / replayed snapshots.
6. Deduplication on Bronze uses an auto-optimizing `MERGE` to drop exact duplicate vectors within a batch.

> This pattern is a faithful stand-in for the ingestion of operational state feeds typical of an aviation data platform, while keeping cost inside free-tier limits.

### 2.4 Data-Quality Strategy

Data quality is **governance-by-design**: every DLT table declares `CONSTRAINT` / `expect` rules whose outcomes are routed to either `DROP` (Bronze quarantine) or `FAIL` (Silver). Quality metrics are materialized in `governance.dq_results` for lineage and dashboards.

| Layer | Representative Expectations |
|---|---|
| Bronze | `icao24 IS NOT NULL`; `time_position > 0`; valid ISO country string |
| Silver | `longitude BETWEEN -180 AND 180`; `latitude BETWEEN -90 AND 90`; `baro_altitude >= -1000`; `velocity >= 0`; dedup key unique |
| Gold | `current_flights`: 1 row per `icao24`; `airport_congestion`: window aligned to UTC minute |

### 2.5 Governance & Security (Unity Catalog)

- **Metastore**: single Unity Catalog metastore linked to the GCP workspace.
- **Catalogs**: `flight_cat` (prod) and `flight_cat_dev` (sandbox) — same model, separate data.
- **Schemas**: `bronze`, `silver`, `gold`, `governance`, `ml_features`.
- **Storage credentials / external locations**: backed by a GCS service account; Terraform-managed.
- **Grants**: role-based principals (`de_reader`, `de_writer`, `de_admin`) with least privilege.
- **Tags**: `PII=false`, `domain=flight_ops`, `layer=bronze|silver|gold`, `sla_min=15`.
- **Lineage**: auto-captured by Unity Catalog across DLT; visualized in Catalog Explorer.
- **Lifecycle**: `ALTER TABLE … SET TBLPROPERTIES (delta.logRetentionDuration, deletedFileRetentionDuration)` per layer.

### 2.6 Infrastructure as Code (Terraform)

All platform resources are defined in Terraform so the whole environment is reproducible and disposable — matching the "startup-like delivery on enterprise-grade foundations" competency.

```
infra/
├── terraform/
│   ├── versions.tf              # provider pinning: databricks, google
│   ├── backend.tf               # remote state (GCS) — optional for sandbox
│   ├── variables.tf             # project_id, region, prefix, etc.
│   ├── providers.tf
│   ├── databricks.tf            # workspace, UC metastore, catalog, schemas
│   ├── storage.tf               # GCS bucket + service account for UC
│   ├── unity_catalog.tf         # storage creds, external locations, grants, tags
│   ├── jobs.tf                  # DLT pipeline + ingestion job + workflow
│   ├── clusters.tf              # job clusters / serverless SQL warehouse
│   ├── policies.tf              # cluster policies for cost control
│   └── outputs.tf
└── modules/
    └── (reusable: uc_grants, dlt_pipeline)
```

Key Terraform-managed objects:
- Databricks workspace on GCP.
- GCS bucket + service account + storage credential + external location.
- Unity Catalog metastore assignment, `flight_cat` catalog, schemas, and grants.
- DLT pipeline definition (Python/SQL source-file-backed, no notebooks) with `edition = "CORE"` and serverless where supported.
- Streaming ingestion Databricks job (Python script + job cluster).
- Orchestration workflow chaining: `ingest → dlt_update → gold_refresh`.
- Serverless SQL Warehouse (small) for BI consumption.
- Cluster policies enforcing max DBUs to stay in free/trial tier.

### 2.7 Repository Structure

```
flight-project/
├── docs/
│   └── project_overview.md          # this file
├── infra/
│   └── terraform/                   # IaC (see 2.6)
├── src/
│   ├── dlt/
│   │   ├── bronze_pipeline.py       # streaming bronze definition
│   │   ├── silver_pipeline.py       # cleaning + dedup + watermark
│   │   └── gold_pipeline.py         # data products
│   ├── ingestion/
│   │   └── opensky_stream.py        # Structured Streaming foreachBatch job script
│   ├── jobs/
│   │   ├── gold_refresh.py          # Gold-table materialization job
│   │   ├── dq_publish.py           # DQ metrics publishing job
│   │   └── ml_features.py          # Feature table refresh job
│   └── utils/
│       ├── opensky_client.py
│       └── schema.py
├── tests/
│   ├── test_opensky_client.py
│   └── test_schema.py
├── .github/workflows/                # CI: fmt, validate, lint, pytest
├── Makefile
├── pyproject.toml
└── README.md
```

### 2.8 Orchestration

A single **Databricks Workflow** orchestrates the platform lifecycle:

```
[ingest_streaming_job (continuous)]
        │
        ▼
[dlt_pipeline.update()]  ──▶  [gold_refresh_job]  ──▶  [dq_publish_job]
        │                                                   │
        └──────────── lineage ─────────────────────────────▶ governance.dq_results
```

- The streaming job runs **continuously** (or on a `PER_MINUTE` trigger to respect free-tier limits).
- The DLT pipeline runs on a 5-minute schedule (`triggered` update mode).
- `gold_refresh_job` materializes Gold tables and emits feature rows.
- `dq_publish_job` copies DLT event logs / expectation metrics into `governance.dq_results`.

### 2.9 Consumption & ML Readiness

- **Power BI / SQL Warehouse**: Gold tables are queryable via a small serverless SQL Warehouse; a thin semantic model (`measures`: active flights, avg altitude; `dimensions`: icao24, callsign, origin_country, hour) is documented.
- **MLflow Feature Store stub**: `ml_features.flight_features` exposes a feature table suitable for a future "flight delay" or "anomaly" model, demonstrating the platform's AI/ML-enablement capabilities.

### 2.10 Cost & Free-Tier Guardrails

- Use **DLT Core** edition (no Premium ML features needed for the core demo).
- Serverless where available; otherwise single-node job clusters with auto-scale off.
- Cluster policies cap `max_workers = 1` and `node_type_id` to a small machine.
- Streaming trigger interval tuned to OpenSky anonymous rate limits (≥ 10 s).
- `terraform destroy` tears down the entire stack to stop spend.

---

## 3. Technical-Lead Alignment Notes

This sandbox is deliberately scoped to demonstrate the competencies expected of a Technical Lead owning an enterprise data platform:

- **"Hands-on"**: the code lives in `src/`, not just slides; Terraform is the only way to create resources.
- **"Governance-by-design"**: Unity Catalog is provisioned before any table exists; lineage and tags are part of the pipeline, not an afterthought.
- **"Streaming + batch"**: Structured Streaming ingestion feeds DLT batches, showing both paradigms.
- **"Pragmatic architecture"**: a single-person deliverable that still models separation of concerns, DQ, and IaC.
- **"Mentoring & standards"**: repo conventions, CI, modular job scripts, and documented patterns can be handed to future engineers.
- **"Aviation domain"**: OpenSky is the closest free analog to real-world aircraft operational feeds.
