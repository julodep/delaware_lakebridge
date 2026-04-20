# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIRouteOperationStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIRouteOperationStaging.Table.sql`

# COMMAND ----------

# -------------------------------------------------------------------------
# Create Delta Lake table `dbe_dbx_internships`.`dbo`.`SMRBIRouteOperationStaging`
# -------------------------------------------------------------------------
# NOTE: Delta Lake does not support a true PRIMARY KEY constraint.  
# The original T‑SQL primary‑key definition is retained only as a comment
# for reference.  If you require uniqueness enforcement, consider adding
# a unique index in downstream processing or using a materialized view.

spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIRouteOperationStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID     STRING NOT NULL,
    ISSELECTED      INT    NOT NULL,
    TRANSFERSTATUS  INT    NOT NULL,
    OPERATIONNAME   STRING NOT NULL,
    OPERATIONID     STRING NOT NULL,
    COMPANY         STRING NOT NULL,
    PARTITION       STRING NOT NULL,
    DATAAREAID      STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL
)
USING delta
""")

# COMMAND ----------

# -------------------------------------------------------------------------
# Optional: Validate table creation
# -------------------------------------------------------------------------
spark.sql(f"DESCRIBE `dbe_dbx_internships`.`dbo`.`SMRBIRouteOperationStaging`").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
