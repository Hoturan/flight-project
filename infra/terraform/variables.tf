# Input variables for the flight-project Databricks platform.
# Databricks Free Edition (AWS) — no cloud provider variables needed.

variable "workspace_url" {
  description = "Existing Databricks workspace URL"
  type        = string
}

variable "workspace_path" {
  description = "Absolute path to the repo in Databricks Workspace (e.g. /Workspace/Users/you/flight-project or /Repos/flight-project)"
  type        = string
}

variable "databricks_client_id" {
  description = "Databricks service-principal client ID for provider auth."
  type        = string
  sensitive   = true
}

variable "databricks_client_secret" {
  description = "Databricks service-principal client secret for provider auth."
  type        = string
  sensitive   = true
}

variable "prefix" {
  description = "Prefix applied to all resource names for uniqueness."
  type        = string
  default     = "flight"
}

variable "catalog_name" {
  description = "Unity Catalog catalog name for the flight platform."
  type        = string
  default     = "flight_cat"
}

variable "dlt_edition" {
  description = "DLT pipeline edition: CORE, PRO, or PRO_PREMIUM."
  type        = string
  default     = "PRO"
}

variable "poll_interval_seconds" {
  description = "OpenSky polling interval in seconds (anonymous rate limit ≥ 10s)."
  type        = number
  default     = 10
}