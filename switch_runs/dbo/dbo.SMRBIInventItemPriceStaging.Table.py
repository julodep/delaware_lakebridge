# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIInventItemPriceStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIInventItemPriceStaging.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------- #
# 1️⃣  Create the staging table in the target catalogue & schema   #
#     The original T‑SQL used *IF NOT EXISTS*, a clustered primary  #
#     key and storage options that are not supported by Delta Lake. #
#     Spark SQL therefore ignores the PRIMARY KEY clause and       #
#     on‑premises storage options.  The table is created with the   #
#     same column names and data types, but the constraint is     #
#     documented in a comment for future reference.              #
# --------------------------------------------------------------- #

create_table_sql = f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIInventItemPriceStaging` (
    DEFINITIONGROUP   STRING  NOT NULL,
    EXECUTIONID       STRING  NOT NULL,
    ISSELECTED        INT     NOT NULL,
    TRANSFERSTATUS    INT     NOT NULL,
    ACTIVATIONDATE    TIMESTAMP NOT NULL,
    COSTINGTYPE       INT     NOT NULL,
    ITEMPRICECREATEDDATETIME TIMESTAMP NOT NULL,
    COMPANY           STRING  NOT NULL,
    INVENTDIMID       STRING  NOT NULL,
    ITEMID            STRING  NOT NULL,
    MARKUP            DECIMAL(32,16) NOT NULL,
    PRICE             DECIMAL(32,16) NOT NULL,
    PRICECALCID       STRING  NOT NULL,
    ITEMPRICEMODIFIEDDATETIME TIMESTAMP NOT NULL,
    PRICEQTY           DECIMAL(32,6) NOT NULL,
    PRICETYPE          INT     NOT NULL,
    PRICEUNIT          DECIMAL(32,12) NOT NULL,
    INVENTITEMPRICERECID BIGINT  NOT NULL,
    STDCOSTTRANSDATE   TIMESTAMP NOT NULL,
    STDCOSTVOUCHER     STRING  NOT NULL,
    UNITID              STRING  NOT NULL,
    VERSIONID           STRING  NOT NULL,
    PARTITION           STRING  NOT NULL,
    DATAAREAID          STRING  NOT NULL,
    SYNCSTARTDATETIME   TIMESTAMP NOT NULL
    /*  
       The following PRIMARY KEY was part of the original T‑SQL:
           CONSTRAINT PK_SMRBIInventItemPriceStaging
               PRIMARY KEY (EXECUTIONID, INVENTITEMPRICERECID, DATAAREAID, PARTITION)
       Delta Lake does not enforce PRIMARY KEY constraints, but the
       columns are kept exactly as they appeared in the source schema.
    */
);
"""

# COMMAND ----------

spark.sql(create_table_sql)

# COMMAND ----------

# --------------------------------------------------------------- #
# 2️⃣  Optional: Verify the table was created successfully.       #
# --------------------------------------------------------------- #

spark.sql(f"DESCRIBE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIInventItemPriceStaging`").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
