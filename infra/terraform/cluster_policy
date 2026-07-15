# Cluster policy enforcing free-tier cost guardrails.
# Caps workers and node size to keep spend inside Databricks Free Edition limits.

resource "databricks_cluster_policy" "free_tier" {
  provider = databricks.workspace
  name     = "${var.prefix}-free-tier-policy"
  definition = jsonencode({
    "spark_conf.spark.databricks.delta.preview.enabled" : {
      "type" : "fixed",
      "value" : "true"
    },
    "node_type_id" : {
      "type" : "allowlist",
      "values" : ["i3.xlarge", "i3.2xlarge", "m5.xlarge", "m5.2xlarge"],
      "defaultValue" : "i3.xlarge"
    },
    "num_workers" : {
      "type" : "range",
      "minValue" : 0,
      "maxValue" : 1,
      "defaultValue" : 0
    },
    "cluster_source" : {
      "type" : "allowlist",
      "values" : ["JOB"]
    }
  })

  depends_on = [databricks_schema.schemas]
}