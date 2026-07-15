# Databricks Jobs for Databricks Free Edition (AWS).
# Uses notebook_task (thin wrappers) instead of spark_python_task.
# All file paths must be absolute (prefixed with /Workspace/).
#
# The DLT pipeline handles Bronze → Silver → Gold.
# No separate gold_refresh job is needed.

# 1. Batch ingestion job
resource "databricks_job" "ingest_streaming" {
  provider = databricks.workspace
  name     = "${var.prefix}-ingest-streaming"

  task {
    task_key = "ingest"
    notebook_task {
      notebook_path = "${var.workspace_path}/src/notebooks/run_ingest"
      base_parameters = {
        catalog         = var.catalog_name
        poll_interval   = tostring(var.poll_interval_seconds)
        workspace_path  = var.workspace_path
      }
    }
  }

  schedule {
    quartz_cron_expression = "0 */1 * * * ?"
    timezone_id            = "UTC"
  }

  depends_on = [databricks_schema.schemas]
}

# 2. DLT pipeline (Bronze → Silver → Gold)
resource "databricks_pipeline" "dlt_pipeline" {
  provider   = databricks.workspace
  name       = "${var.prefix}-dlt-pipeline"
  edition    = var.dlt_edition
  channel    = "CURRENT"
  serverless = true

  catalog = var.catalog_name
  target  = "bronze"

  library {
    file {
      path = "${var.workspace_path}/src/dlt/bronze_pipeline.py"
    }
  }

  library {
    file {
      path = "${var.workspace_path}/src/dlt/silver_pipeline.py"
    }
  }

  library {
    file {
      path = "${var.workspace_path}/src/dlt/gold_pipeline.py"
    }
  }

  depends_on = [databricks_schema.schemas]
}

# 3. DQ metrics publish job
resource "databricks_job" "dq_publish" {
  provider = databricks.workspace
  name     = "${var.prefix}-dq-publish"

  task {
    task_key = "dq_publish"
    notebook_task {
      notebook_path = "${var.workspace_path}/src/notebooks/run_dq_publish"
      base_parameters = {
        catalog        = var.catalog_name
        workspace_path = var.workspace_path
      }
    }
  }

  schedule {
    quartz_cron_expression = "0 */5 * * * ?"
    timezone_id            = "UTC"
  }

  depends_on = [databricks_pipeline.dlt_pipeline]
}

# 4. Orchestration workflow: DLT → dq_publish
resource "databricks_job" "orchestration" {
  provider = databricks.workspace
  name     = "${var.prefix}-orchestration"

  task {
    task_key = "trigger_dlt"
    pipeline_task {
      pipeline_id = databricks_pipeline.dlt_pipeline.id
    }
  }

  task {
    task_key = "dq_publish"
    depends_on {
      task_key = "trigger_dlt"
    }
    notebook_task {
      notebook_path = "${var.workspace_path}/src/notebooks/run_dq_publish"
      base_parameters = {
        catalog        = var.catalog_name
        workspace_path = var.workspace_path
      }
    }
  }

  schedule {
    quartz_cron_expression = "0 */5 * * * ?"
    timezone_id            = "UTC"
  }

  depends_on = [databricks_pipeline.dlt_pipeline]
}
