# Terraform Infrastructure — Databricks on GCP

This directory contains all Terraform configuration to provision the flight-project data platform on Databricks (GCP).

---

## Quick Reference — Basic Terraform Commands

All commands assume you are in the `infra/terraform/` directory (or use the `Makefile` targets from the project root).

| Command | What it does | Makefile target |
|---|---|---|
| `terraform init` | Download providers and initialize backend | `make terraform-init` |
| `terraform init -backend=false` | Init without remote state (for CI validation) | `make validate` |
| `terraform fmt -recursive` | Auto-format all `.tf` files | `make fmt` |
| `terraform fmt -check -recursive` | Check formatting without changing files (CI) | — |
| `terraform validate` | Validate configuration syntax and provider schemas | `make validate` |
| `terraform plan` | Show the execution plan (what will be created/changed/destroyed) | `make terraform-plan` |
| `terraform apply -auto-approve` | Apply the plan and create/update resources | `make terraform-apply` |
| `terraform destroy -auto-approve` | Destroy all managed resources (tear everything down) | `make terraform-destroy` |
| `terraform output` | Print current output values (workspace URL, pipeline IDs, etc.) | — |
| `terraform state list` | List all resources currently in the state | — |
| `terraform graph` | Visualise the dependency graph | — |

### Typical workflow

```bash
# 1. Copy the tfvars template and fill in your values
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your GCP project, Databricks credentials, etc.

# 2. Initialize
terraform init

# 3. Review the plan
terraform plan

# 4. Apply
terraform apply -auto-approve

# 5. Get outputs (workspace URL, pipeline IDs)
terraform output

# 6. Tear down when done (stops all spend)
terraform destroy -auto-approve
```

---

## File-by-File Description

### `versions.tf`
Pins the Terraform version (`>= 1.5.0`) and the required providers:
- `databricks/databricks` (`~> 1.40`) — needed for Unity Catalog on GCP.
- `hashicorp/google` (`~> 4.80`) — GCS, IAM, VPC.
- `hashicorp/random` (`~> 3.5`) — random suffixes for unique resource names.

### `backend.tf`
Configures remote state storage in GCS (commented out by default). Uncomment and fill in the bucket name + prefix to enable remote state. For a single-developer sandbox, local state is fine.

### `variables.tf`
Declares all input variables:
- `project_id` — GCP project ID (required).
- `region` — GCP region for workspace and GCS bucket.
- `prefix` — Name prefix for all resources.
- `databricks_account_id`, `databricks_client_id`, `databricks_client_secret` — Databricks service-principal credentials for the Terraform provider.
- `google_credentials` — Path to the GCP service-account JSON key.
- `catalog_name` — Unity Catalog catalog name (`flight_cat`).
- `dlt_edition` — DLT pipeline edition (`CORE`, `PRO`, `PRO_PREMIUM`).
- `poll_interval_seconds` — OpenSky polling interval (≥ 10s for anonymous rate limits).

### `providers.tf`
Configures the Google and Databricks Terraform providers:
- **Google provider** — authenticates with `google_credentials` for GCS/IAM/VPC operations.
- **Databricks account-level provider** (`alias = "account"`) — used for workspace creation and UC metastore management.
- **Databricks workspace-level provider** (`alias = "workspace"`) — used for catalog, schemas, jobs, pipelines within the workspace. Resolved after the workspace is created.

### `storage.tf`
Provisions the GCS storage layer:
- `random_string.suffix` — ensures globally-unique bucket names.
- `google_service_account.uc_sa` — the service account Databricks uses to access GCS via Unity Catalog.
- `google_storage_bucket.delta_root` — the GCS bucket that becomes the Delta storage root. Has a 30-day object lifecycle rule and uniform bucket-level access.
- `google_storage_bucket_iam_member` grants — give the UC service account `objectAdmin` and `legacyBucketReader` roles on the bucket.

### `databricks.tf`
Provisions the Databricks workspace and Unity Catalog:
- `google_compute_network.vpc` + `google_compute_subnetwork.subnet` — VPC for the workspace.
- `google_service_account.workspace_sa` — service account for the Databricks workspace.
- `databricks_mws_workspaces.this` — the Databricks workspace on GCP.
- `databricks_uc_metastore.this` — the Unity Catalog metastore pointing at the GCS storage root.
- `databricks_uc_metastore_assignment.this` — links the metastore to the workspace.
- `databricks_catalog.flight` — the `flight_cat` catalog.
- `databricks_schema.schemas` — creates the `bronze`, `silver`, `gold`, `governance`, and `ml_features` schemas.

### `unity_catalog.tf`
Configures Unity Catalog governance objects:
- `databricks_storage_credential.gcs_uc` — registers the GCS service account as a UC storage credential.
- `databricks_external_location.delta_root` — maps the GCS bucket as an external location.
- `databricks_grants.catalog_grants` — role-based grants (`de_reader`, `de_writer`, `de_admin`) on the catalog.
- `databricks_catalog.flight_tags` — metadata/governance tags on the catalog.

### `cluster_policy.tf`
Defines a cost-capping cluster policy (`flight-free-tier-policy`):
- Restricts `node_type_id` to small machine types (`n1-standard-4`, `n1-standard-8`, `n2-standard-4`, `n2-standard-8`).
- Caps `num_workers` at 0–1 (single-node or 1-worker clusters only).
- Forces `cluster_source = JOB` (no interactive clusters).
- All Databricks jobs and DLT clusters reference this policy to enforce cost control.

### `jobs.tf`
Defines all Databricks jobs and the DLT pipeline:
1. **`databricks_job.ingest_streaming`** — Structured Streaming ingestion job (`src/ingestion/opensky_stream.py`), scheduled every minute.
2. **`databricks_pipeline.dlt_pipeline`** — Delta Live Tables pipeline (`src/dlt/bronze_pipeline.py`, `silver_pipeline.py`, `gold_pipeline.py`), CORE edition.
3. **`databricks_job.gold_refresh`** — Gold table materialization job (`src/jobs/gold_refresh.py`), scheduled every 5 minutes.
4. **`databricks_job.dq_publish`** — Data-quality metrics publishing job (`src/jobs/dq_publish.py`), scheduled every 5 minutes.
5. **`databricks_job.orchestration`** — Orchestration workflow chaining: `trigger_dlt → gold_refresh → dq_publish`, scheduled every 5 minutes.

### `sql_warehouse.tf`
Provisions a small **serverless SQL Warehouse** for BI consumption:
- `databricks_sql_endpoint.consumption` — Small cluster size, auto-stops after 10 minutes, serverless enabled.
- Used by Power BI / direct SQL queries against the Gold-layer tables.

### `outputs.tf`
Exports useful values after `terraform apply`:
- `workspace_url` — Databricks workspace URL.
- `workspace_id` — Workspace ID.
- `gcs_bucket` — GCS bucket name (Delta storage root).
- `catalog_name` — Unity Catalog catalog name.
- `dlt_pipeline_id` — DLT pipeline ID.
- `sql_warehouse_id` — SQL Warehouse endpoint ID.
- `cluster_policy_id` — Cluster policy ID.

### `terraform.tfvars.example`
A template showing all required variables. Copy to `terraform.tfvars` and fill in your values. The `terraform.tfvars` file is gitignored and must never be committed.

---

## Important Notes

- **State management**: for a sandbox, local state is fine. For team collaboration, uncomment the GCS backend in `backend.tf`.
- **Sensitive values**: all credentials in `terraform.tfvars` are marked `sensitive = true` in `variables.tf`. The `terraform.tfvars` file is gitignored.
- **Cost**: `terraform destroy` tears down everything — workspace, GCS bucket, all jobs. Use it to stop all spend.
- **Provider ordering**: the Databricks workspace provider depends on the workspace being created first. Terraform handles this via implicit dependencies (`databricks_mws_workspaces.this.workspace_url`).