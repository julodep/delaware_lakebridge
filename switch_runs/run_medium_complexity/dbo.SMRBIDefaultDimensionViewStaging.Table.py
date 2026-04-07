# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIDefaultDimensionViewStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/baseline_scripts_medium/dbo.SMRBIDefaultDimensionViewStaging.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------
# 1️⃣  Setup: import required modules (Databricks provides a spark session).
# --------------------------------------------------------------
from pyspark.sql import functions as F   # unused in creation but handy for later work

# COMMAND ----------

# --------------------------------------------------------------
# 2️⃣  Create the table `SMRBIDefaultDimensionViewStaging` in the target Delta catalog.
#     The original T‑SQL used dbo schema and various SQL Server specific
#     clauses (`SET ANSI_NULLS`, `SET QUOTED_IDENTIFIER`, `ON [PRIMARY]` and
#     a clustered PRIMARY KEY).  In Databricks / Delta Lake these options
#     are either unnecessary or not supported, so they are omitted.  The
#     definition below contains the equivalent Spark/Delta column types.
# --------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS dbe_dbx_internships.switchschema.SMRBIDefaultDimensionViewStaging (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    DISPLAYVALUE STRING NOT NULL,
    DEFAULTDIMENSION LONG NOT NULL,
    NAME STRING NOT NULL,
    PARTITION STRING NOT NULL,
    YSLEGROUPDIMENSION STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID LONG NOT NULL
)
""")

# COMMAND ----------

# --------------------------------------------------------------
# 3️⃣  Note on primary key
#     ----
#     The original T‑SQL declared a clustered PRIMARY KEY on several columns.
#     Delta Lake does not enforce primary key constraints; you can enforce
#     uniqueness using custom validation logic or by creating a supplemental
#     index (e.g., using Delta Lake's OPTIMIZE, ZORDER).  For the purposes
#     of this conversion the primary key definition is omitted.
# --------------------------------------------------------------

# --------------------------------------------------------------
# 4️⃣  (Optional) Verify table creation by showing the schema
# --------------------------------------------------------------
spark.sql("DESCRIBE dbe_dbx_internships.switchschema.SMRBIDefaultDimensionViewStaging").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
