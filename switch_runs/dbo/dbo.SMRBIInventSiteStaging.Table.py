# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIInventSiteStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIInventSiteStaging.Table.sql`

# COMMAND ----------

# -------------------------------------------------------------
# NOTE: This notebook is intended to run on Databricks using
#       the catalog `dbe_dbx_internships` and the schema `dbo`.
#       All object references are fully‑qualified:
#       `dbe_dbx_internships`.`dbo`.`SMRBIInventSiteStaging`
# -------------------------------------------------------------

# Spark SQL can create a Delta table directly.
# The primary‑key declaration in T‑SQL is ignored because Delta Lake
# does not support primary‑key constraints natively.  If a logical
# uniqueness property is required, it can be enforced via a
# *unique* constraint (available in recent Unity Catalog releases)
# or by handling it through application logic.

# ---------------------------------------
# 1. Create the SMRBIInventSiteStaging table
# ---------------------------------------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIInventSiteStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID   STRING NOT NULL,
    ISSELECTED    INT    NOT NULL,
    TRANSFERSTATUS INT   NOT NULL,
    NAME          STRING NOT NULL,
    SITEID        STRING NOT NULL,
    COMPANY       STRING NOT NULL,
    `PARTITION`   STRING NOT NULL,
    DATAAREAID    STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL
) USING DELTA
""")

# COMMAND ----------

# ---------------------------------------
# 2. (Optional) Enforce uniqueness
# ---------------------------------------
# The original T‑SQL created a primary key on
# (EXECUTIONID, SITEID, DATAAREAID, PARTITION).
# If your Unity Catalog version supports unique constraints,
# you can add them explicitly:
#
# spark.sql(f"""
# ALTER TABLE `dbe_dbx_internships`.`dbo`.`SMRBIInventSiteStaging`
#   ADD CONSTRAINT uq_smrbi_invest_site_staging_unique
#   UNIQUE (EXECUTIONID, SITEID, DATAAREAID, PARTITION)
# """)
#
# If the unique constraint feature is not available,
# simply rely on your application logic to guarantee
# uniqueness when inserting or updating rows.

# ---------------------------------------
# 3. Verify table creation
# ---------------------------------------
spark.sql(f"DESCRIBE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIInventSiteStaging`").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
