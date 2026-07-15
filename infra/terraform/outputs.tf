# Useful outputs for connecting to the provisioned platform.

output "workspace_url" {
  description = "Databricks workspace URL."
  value       = var.workspace_url
}

output "catalog_name" {
  description = "Unity Catalog catalog name."
  value       = var.catalog_name
}

output "dlt_pipeline_id" {
  description = "DLT pipeline ID."
  value       = databricks_pipeline.dlt_pipeline.id
}

output "sql_warehouse_id" {
  description = "SQL Warehouse endpoint ID for BI consumption."
  value       = databricks_sql_endpoint.consumption.id
}

output "cluster_policy_id" {
  description = "Cluster policy ID for cost-controlled job clusters."
  value       = databricks_cluster_policy.free_tier.id
}