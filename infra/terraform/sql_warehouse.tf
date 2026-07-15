# SQL Warehouse for BI consumption.
# COMMENTED OUT: Databricks Free Edition has a warehouse limit.
# If you have no existing warehouses, uncomment this block.
# Otherwise, use the existing warehouse in your workspace.

# resource "databricks_sql_endpoint" "consumption" {
#   provider             = databricks.workspace
#   name                 = "${var.prefix}-sql-warehouse"
#   cluster_size         = "2X-Small"
#   min_num_clusters     = 1
#   max_num_clusters     = 1
#   auto_stop_mins       = 10
#   enable_serverless_compute = false
# 
#   depends_on = [databricks_schema.schemas]
# }
