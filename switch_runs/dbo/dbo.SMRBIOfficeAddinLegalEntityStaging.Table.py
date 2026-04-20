# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIOfficeAddinLegalEntityStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIOfficeAddinLegalEntityStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create the staging table for SMRBIOfficeAddinLegalEntityStaging
# ------------------------------------------------------------------
# The following DDL creates a Delta table that mirrors the original
# T‑SQL table. All column types are mapped to Spark SQL types and
# the fully‑qualified name `dbe_dbx_internships.dbo.SMRBIOfficeAddinLegalEntityStaging`
# is used as required by the policy.
#
# Note: Delta Lake does **not** support native PRIMARY KEY constraints.
# The original T‑SQL primary‑key definition has been omitted. If you
# need to enforce uniqueness you must handle it at the application
# level or use a separate unique‑index table.
#

spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIOfficeAddinLegalEntityStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    COMPANYNAME STRING NOT NULL,
    COMPANYID STRING NOT NULL,
    BUSINESSACTIVITY STRING NOT NULL,
    PARTITION STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID BIGINT NOT NULL
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
