# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBILedgerChartOfAccountsStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBILedgerChartOfAccountsStaging.Table.sql`

# COMMAND ----------

# ---------------------------------------------------------------
# 1️⃣ Create the staging table in the target Delta Lake catalog.
# ---------------------------------------------------------------
# •  All fully‑qualified object names use dbe_dbx_internships.dbo.{object_name}.
# •  Data types are mapped from T‑SQL to Spark‑SQL:
#     * NVARCHAR(60) → STRING
#     * INT           → INT
#     * BIGINT        → BIGINT
#     * DATETIME      → TIMESTAMP
# •  The original table definition included a PRIMARY KEY and a clustered index.
#     Delta Lake does **not** enforce primary keys or clustered indexes
#     (it uses unstructured partitions).  
#     We keep the column list and a comment indicating the original
#     constraints; they can be enforced at the application level if needed.
#
catalog = "<YOUR_CATALOG>"   # replace with your Databricks catalog name
schema  = "<YOUR_SCHEMA>"    # replace with your Databricks schema name

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBILedgerChartOfAccountsStaging` (
      DEFINITIONGROUP STRING NOT NULL,
      EXECUTIONID   STRING NOT NULL,
      ISSELECTED    INT   NOT NULL,
      TRANSFERSTATUS INT   NOT NULL,
      DESCRIPTION   STRING NOT NULL,
      CHARTOFACCOUNTS STRING NOT NULL,
      CHARTOFACCOUNTSRECID BIGINT NOT NULL,
      LEDGERCHARTOFACCOUNTSRECID BIGINT NOT NULL,
      PARTITION      STRING NOT NULL,
      SYNCSTARTDATETIME TIMESTAMP NOT NULL,
      RECID          BIGINT NOT NULL
  )
  -- The original T‑SQL version defined a composite PRIMARY KEY on
  -- (EXECUTIONID, CHARTOFACCOUNTS, PARTITION) and a clustered index.
  -- Delta Lake does not support native primary‑key enforcement.  If unique
  # constraints are required you can drop duplicates during data ingestion
  # or use a Delta Lake "unique index" recipe (e.g., a CHECK or Spark SQL
  # validation step).  For simplicity we omit the constraint here.
""")

# COMMAND ----------

# ---------------------------------------------------------------
# 2️⃣ Verify the table was created (optional)
# ---------------------------------------------------------------
spark.sql(f"DESCRIBE TABLE `dbe_dbx_internships`.`dbo`.`SMRBILedgerChartOfAccountsStaging`").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
