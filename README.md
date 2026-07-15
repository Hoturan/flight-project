# Flight Project — OpenSky Real-Time Aviation Data Platform on Databricks

A hands-on engineering sandbox that builds a production-grade, governed, streaming data platform on Databricks. It ingests **real-time aircraft positional data** from the [OpenSky Network](https://opensky-network.org/) REST API, processes it through a medallion architecture (Bronze → Silver → Gold), and exposes curated **data products** for analytics, operational consumption, and AI/ML enablement.

> **OpenSky endpoint:** `https://opensky-network.org/api/states/all`

---

## Quick Start

### Prerequisites

- [Terraform](https://developer.hashicorp.com/terraform/downloads) >= 1.5
- [Google Cloud CLI](https://cloud.google.com/sdk/docs/install) (`gcloud`)
- A Databricks account on GCP (free / community / trial tier is sufficient)
- Python >= 3.10 with `pip`

### 1. Clone & Explore

```bash
git clone <repo-url> flight-project
cd flight-project
```

### 2. Provision Infrastructure with Terraform

```bash
# Set required variables (via terraform.tfvars or -var flags)
cat > infra/terraform/terraform.tfvars <<EOF
project_id               = "your-gcp-project-id"
region                   = "europe-west1"
databricks_account_id    = "your-databricks-account-id"
databricks_client_id     = "your-sp-client-id"
databricks_client_secret = "your-sp-client-secret"
google_credentials       = "/path/to/gcp-service-account.json"
EOF

make terraform-init
make terraform-plan
make terraform-apply
```

This creates:
- A Databricks workspace on GCP
- A GCS bucket as the Delta storage root
- A Unity Catalog metastore, catalog (`flight_cat`), and schemas
- Storage credentials, external locations, and grants
- A DLT pipeline, streaming ingestion job, orchestration workflow
- A serverless SQL Warehouse for BI consumption
- A cost-capping cluster policy

### 3. Run Tests Locally

```bash
pip install pytest ruff requests pyspark
make test
```

### 4. Tear Down

```bash
make terraform-destroy
```

---

## Architecture

See [`docs/project_overview.md`](docs/project_overview.md) for the full architecture, medallion data model, streaming design, data-quality strategy, and governance model.

```
OpenSky REST → Structured Streaming (foreachBatch) → bronze.raw_states
                                                       ↓
                          DLT Pipeline (no notebooks)
                       bronze → silver → gold
                          ↓        ↓       ↓
                   current_flights | airport_congestion | anomaly_feed
                          ↓
                   ml_features.flight_features
                          ↓
                   SQL Warehouse / Power BI
```

---

## Repository Structure

```
flight-project/
├── docs/
│   └── project_overview.md          # architecture & project definition
├── infra/
│   └── terraform/                   # IaC (workspace, UC, jobs, policies)
├── src/
│   ├── dlt/                         # Delta Live Tables (Python source files)
│   │   ├── bronze_pipeline.py
│   │   ├── silver_pipeline.py
│   │   └── gold_pipeline.py
│   ├── ingestion/
│   │   └── opensky_stream.py        # Structured Streaming job
│   ├── jobs/
│   │   ├── gold_refresh.py          # Gold materialization job
│   │   ├── dq_publish.py            # DQ metrics publishing job
│   │   └── ml_features.py           # Feature table refresh job
│   └── utils/
│       ├── opensky_client.py        # HTTP client for OpenSky API
│       └── schema.py                # PySpark schema definitions
├── tests/
│   ├── test_opensky_client.py
│   └── test_schema.py
├── .github/workflows/               # CI
├── Makefile
├── pyproject.toml
└── README.md
```

---

## Key Design Decisions

- **No notebooks**: all transformation logic lives in versioned Python/SQL source files executed by Databricks Jobs and Workflows.
- **Governance-by-design**: Unity Catalog is provisioned before any table exists; lineage, tags, and grants are part of the pipeline.
- **Medallion architecture**: Bronze (raw append) → Silver (typed, deduped, watermarked) → Gold (data products).
- **Structured Streaming**: a `rate` source drives micro-batch polling of the OpenSky REST endpoint via `foreachBatch`.
- **Cost guardrails**: cluster policies cap workers and node sizes to stay within free/trial limits.

---

## License

This project is for educational and interview-preparation purposes.