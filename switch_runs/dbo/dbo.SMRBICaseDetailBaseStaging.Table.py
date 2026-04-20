# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBICaseDetailBaseStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBICaseDetailBaseStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create the staging table `SMRBICaseDetailBaseStaging` in the
# specified catalog and schema.  The table will be created (or
# replaced if it already exists) in catalog `dbe_dbx_internships` and schema
# `dbo` as a Delta table.
# ------------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBICaseDetailBaseStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    CASEID STRING NOT NULL,
    CATEGORYRECID BIGINT NOT NULL,
    CLOSEDBY STRING NOT NULL,
    CLOSEDDATETIME TIMESTAMP NOT NULL,
    CONTACTPERSONID STRING NOT NULL,
    DEPARTMENT BIGINT NOT NULL,
    DESCRIPTION STRING NOT NULL,
    EMAILID STRING NOT NULL,
    INSTANCERELATIONTYPE BIGINT NOT NULL,
    MEMO STRING,
    OWNERWORKER BIGINT NOT NULL,
    PRIORITY STRING NOT NULL,
    PROCESS STRING NOT NULL,
    STATUS INT NOT NULL,
    CASECATEGORYHIERARCHYDETAIL_CASECATEGORY STRING NOT NULL,
    CASECATEGORYHIERARCHYDETAIL_CATEGORYTYPE INT NOT NULL,
    OMOPERATINGUNIT_PARTYNUMBER STRING NOT NULL,
    OMOPERATINGUNIT_OMOPERATINGUNITTYPE INT NOT NULL,
    ONWERWORKER_PERSONNELNUMBER STRING NOT NULL,
    PARTY_FK_PARTYNUMBER STRING NOT NULL,
    CASEDETAILMODIFIEDBY STRING NOT NULL,
    CASEDETAILMODIFIEDDATETIME TIMESTAMP NOT NULL,
    CASEDETAILMODIFIEDTRANSACTIONID BIGINT NOT NULL,
    CASEDETAILRECVERSION INT NOT NULL,
    PLANNEDEFFECTIVEDATE TIMESTAMP NOT NULL,
    COMPANY STRING NOT NULL,
    CASEDETAILCREATEDBY STRING NOT NULL,
    CASEDETAILCREATEDDATETIME TIMESTAMP NOT NULL,
    CASEDETAILBASERECID BIGINT NOT NULL,
    PARTITION STRING NOT NULL,
    DATAAREAID STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID BIGINT NOT NULL
)
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
