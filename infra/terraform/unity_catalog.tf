# Unity Catalog grants for role-based access.
#
# Grants are optional — the placeholder principals (de_reader, de_writer, de_admin)
# must exist as users/groups/service principals in the workspace before applying.
# Comment out or adjust the grants block if you don't have these principals.

# Uncomment and adjust the principal names below to match your workspace users/groups.
# resource "databricks_grants" "catalog_grants" {
#   provider = databricks.workspace
#   catalog  = var.catalog_name
#
#   grant {
#     principal  = "your-email@example.com"
#     privileges = ["USE_CATALOG", "SELECT", "MODIFY", "CREATE_TABLE", "CREATE_SCHEMA"]
#   }
# }