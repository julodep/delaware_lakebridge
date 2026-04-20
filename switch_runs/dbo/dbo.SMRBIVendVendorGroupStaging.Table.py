# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIVendVendorGroupStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIVendVendorGroupStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# 1️⃣ Create the SMRBIVendVendorGroupStaging table in Databricks
# ------------------------------------------------------------------
# In T‑SQL the table is defined in the dbo schema, but for Delta Lake
# we will create the fully‑qualified table in the target catalog and schema.
#               dbe_dbx_internships.dbo.SMRBIVendVendorGroupStaging
#
# Data type mapping
#   NVARCHAR       → STRING
#   INT            → INT
#   DATETIME       → TIMESTAMP
#   Primary key constraints and WITH(...) options supported by SQL Server
#   are not enforced in Delta Lake, so they are omitted and noted in a
#   comment.
#
# NOTE: If you need application‑level enforcement of uniqueness you can
#       add a `unique` or `primaryKey` constraint in the Delta Lake
#       configuration or handle it in your write logic.
# ------------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIVendVendorGroupStaging`
(
  DEFINITIONGROUP STRING NOT NULL,
  EXECUTIONID   STRING NOT NULL,
  ISSELECTED    INT NOT NULL,
  TRANSFERSTATUS INT NOT NULL,
  VENDORGROUPID STRING NOT NULL,
  DESCRIPTION   STRING NOT NULL,
  COMPANY       STRING NOT NULL,
  PARTITION     STRING NOT NULL,
  DATAAREAID    STRING NOT NULL,
  SYNCSTARTDATETIME TIMESTAMP NOT NULL
)
""")

# COMMAND ----------

# ------------------------------------------------------------------
# 2️⃣ (Optional) Add a comment to remind users about the original primary
#     key definition that Delta Lake can’t enforce directly.
# ------------------------------------------------------------------
spark.sql(f"""
COMMENT ON TABLE `dbe_dbx_internships`.`dbo`.`SMRBIVendVendorGroupStaging`
IS 'Original T‑SQL primary key: EXECUTIONID, VENDORGROUPID, DATAAREAID, PARTITION'
""")

# COMMAND ----------

# ------------------------------------------------------------------
# 3️⃣ Verify that the table was created correctly
# ------------------------------------------------------------------
spark.sql(f"DESC TABLE `dbe_dbx_internships`.`dbo`.`SMRBIVendVendorGroupStaging`").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
