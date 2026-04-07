# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.Calendar.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.Calendar.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------
# This notebook creates the Dimension table Calendar in the
# specified Unity Catalog location.
# --------------------------------------------------------------

# NOTE:
# - The `dbe_dbx_internships` and `datastore` placeholders MUST be replaced
#   with the actual Unity Catalog catalog and schema names
#   when deploying the notebook.
# - All object names are fully‑qualified to avoid ambiguity.
# - The table is defined with the same column names and data
#   types that were specified in the original T‑SQL CREATE TABLE.
# - NOT NULL constraints are preserved (supported in Delta Lake
#   1.5+). If your Databricks runtime does not support
#   constraints, the constraint clauses can be omitted.
# --------------------------------------------------------------

# Drop the table if it already exists (optional but safe).
# spark.sql("DROP TABLE IF EXISTS `dbe_dbx_internships`.`datastore`.`Calendar`")

# Create the Calendar dimension table.
spark.sql(
"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`datastore`.`Calendar` (
    CalendarCode STRING  NOT NULL,
    CalendarName STRING  NOT NULL,
    CompanyCode STRING  NOT NULL,
    StandardWorkDayHours DECIMAL(32,6) NOT NULL
)
"""
)

# COMMAND ----------

# Verify creation by describing the table schema
spark.sql("DESCRIBE `dbe_dbx_internships`.`datastore`.`Calendar`").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
