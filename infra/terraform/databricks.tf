# Databricks Unity Catalog definitions for Free Edition (AWS).
#
# The catalog was already created manually via the Databricks UI.
# We don't manage it via Terraform (to avoid permission issues with the
# service principal). We just create the schemas and grants using the
# catalog name directly.

locals {
  catalog = var.catalog_name
}

# Schemas
resource "databricks_schema" "schemas" {
  for_each     = toset(["bronze", "silver", "gold", "governance", "ml_features"])
  provider     = databricks.workspace
  catalog_name = local.catalog
  name         = each.key
  comment      = "Medallion layer: ${each.key}"
}