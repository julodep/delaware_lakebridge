# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIVendorStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIVendorStaging.Table.sql`

# COMMAND ----------

# Define catalog and schema (replace with actual values)
catalog = "my_catalog"
schema = "my_schema"

# COMMAND ----------

# -------------------------------------------------------------
# 1️⃣ Create the SMRBIVendorStaging table in Delta Lake
# -------------------------------------------------------------
spark.sql(
    f"""
    CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIVendorStaging` (
        DEFINITIONGROUP STRING,
        EXECUTIONID STRING,
        ISSELECTED INT,
        TRANSFERSTATUS INT,
        VENDORACCOUNTNUMBER STRING,
        COMPANYCHAINNAME STRING,
        BUYERGROUPID STRING,
        MAINCONTACTPERSONNELNUMBER STRING,
        ADDRESSCITY STRING,
        ADDRESSCOUNTRYREGIONID STRING,
        ADDRESSVALIDFROM TIMESTAMP,
        ADDRESSVALIDTO TIMESTAMP,
        ADDRESSZIPCODE STRING,
        FORMATTEDPRIMARYADDRESS STRING,
        DEFAULTPURCHASEORDERPOOLID STRING,
        VENDORGROUPID STRING,
        VENDTABLERECID BIGINT,
        COMPANY STRING,
        VENDORDCREATEDDATETIME TIMESTAMP,
        NAME STRING,
        PARTYRECID BIGINT,
        CURRENCY STRING,
        BLOCKED INT,
        VENDORORGANIZATIONNAME STRING,
        PARTITION STRING,
        DATAAREAID STRING,
        SYNCSTARTDATETIME TIMESTAMP
    ) USING delta
    """
)

# COMMAND ----------

# -------------------------------------------------------------
# 2️⃣ Note on PRIMARY KEY
# -------------------------------------------------------------
# The original T‑SQL script declared a clustered primary key on
# (EXECUTIONID, VENDORACCOUNTNUMBER, DATAAREAID, PARTITION).
# Delta Lake does NOT enforce PRIMARY KEY constraints, 
# so the key is documented only for reference.
print(
    """
    # PRIMARY KEY (EXECUTIONID, VENDORACCOUNTNUMBER, DATAAREAID, PARTITION)
    # is NOT enforced in Delta. If uniqueness guarantees are required,
    # they must be handled at the application layer or via a unique
    # index implemented as a Spark SQL constraint (not available in
    # Delta).
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
