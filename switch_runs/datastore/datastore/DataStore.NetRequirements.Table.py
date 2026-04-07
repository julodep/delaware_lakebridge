# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.NetRequirements.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.NetRequirements.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------------------
#  Create the NetRequirements table in Unity Catalog
#  Database objects are qualified as:
#        `dbe_dbx_internships`.`datastore`.`NetRequirements`
#
#  Column type mapping (T‑SQL ► Databricks):
#      BIGINT                    → LONG
#      NVARCHAR(...)             → STRING
#      DATETIME (date & time)    → TIMESTAMP
#      INT                       → INT
#      NUMERIC(p, s)             → DECIMAL(p, s)
#  --------------------------------------------------------------------------

spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`datastore`.`NetRequirements`  -- Persisted Delta table
(
    RecId                 LONG        NOT NULL,  -- BIGINT
    CompanyCode           STRING      NOT NULL,  -- NVARCHAR(4)
    ReferenceType         STRING      NOT NULL,  -- NVARCHAR(255)
    PlanVersion            STRING      NOT NULL,  -- NVARCHAR(10)
    ProductCode           STRING      NOT NULL,  -- NVARCHAR(20)
    InventDimCode         STRING      NOT NULL,  -- NVARCHAR(20)
    RequirementDate       TIMESTAMP   NULL,      -- DATETIME
    RequirementTime      STRING      NOT NULL,  -- VARCHAR(23)
    RequirementDateTime   TIMESTAMP  NULL,      -- DATETIME
    ReferenceCode          STRING      NOT NULL,  -- NVARCHAR(20)
    ProducedItemCode      STRING      NULL,      -- NVARCHAR(20)
    CustomerCode           STRING      NOT NULL,  -- NVARCHAR(20)
    VendorCode             STRING      NOT NULL,  -- NVARCHAR(20)
    ActionDate            TIMESTAMP   NULL,      -- DATETIME
    ActionDays             INT         NOT NULL,  -- INT
    ActionType            STRING      NOT NULL,  -- NVARCHAR(255)
    ActionMarked          STRING      NOT NULL,  -- NVARCHAR(255)
    FuturesDate           TIMESTAMP   NOT NULL,  -- DATETIME
    FuturesDays            INT         NOT NULL,  -- INT
    FuturesCalculated     STRING      NOT NULL,  -- NVARCHAR(255)
    FuturesMarked         STRING      NOT NULL,  -- NVARCHAR(255)
    Direction              STRING      NOT NULL,  -- NVARCHAR(255)
    RankNr                 LONG        NULL,      -- BIGINT
    Quantity               DECIMAL(32,6) NOT NULL,  -- NUMERIC(32,6)
    QuantityConfirmed     DECIMAL(32,6) NOT NULL   -- NUMERIC(32,6)
)
USING DELTA  -- Unity Catalog tables are stored as Delta Lake tables
""")

# COMMAND ----------

# --------------------------------------------------------------------------
#  Optional: Verify that the table was created
# --------------------------------------------------------------------------
spark.sql(f"DESCRIBE TABLE `dbe_dbx_internships`.`datastore`.`NetRequirements`").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
