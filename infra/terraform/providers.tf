# Provider configuration — Databricks only (Free Edition on AWS).
#
# Databricks Free Edition includes a managed metastore, so we only need
# the workspace-level provider for catalog/schema/job/pipeline management.

provider "databricks" {
  alias         = "workspace"
  host          = var.workspace_url
  client_id     = var.databricks_client_id
  client_secret = var.databricks_client_secret
}