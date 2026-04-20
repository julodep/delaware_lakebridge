# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIBOMCalcTableStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIBOMCalcTableStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# 1️⃣  Drop the table if it already exists (idempotent notebook run)
# ------------------------------------------------------------------
spark.sql(f"DROP TABLE IF EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIBOMCalcTableStaging`")

# COMMAND ----------

# ------------------------------------------------------------------
# 2️⃣  Create the table in Delta format (default for Databricks)
# ------------------------------------------------------------------
spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIBOMCalcTableStaging` (
    -- 1. DEFINITIONGROUP  (NVARCHAR(60)) → STRING
    DEFINITIONGROUP          STRING NOT NULL,

    -- 2. EXECUTIONID       (NVARCHAR(90)) → STRING
    EXECUTIONID             STRING NOT NULL,

    -- 3. ISSELECTED        (INT)           → INT
    ISSELECTED              INT NOT NULL,

    -- 4. TRANSFERSTATUS    (INT)           → INT
    TRANSFERSTATUS          INT NOT NULL,

    -- 5. PRICECALCID       (NVARCHAR(20))  → STRING
    PRICECALCID             STRING NOT NULL,

    -- 6. BOMCALCTYPE       (INT)           → INT
    BOMCALCTYPE             INT NOT NULL,

    -- 7. BOMID             (NVARCHAR(20))  → STRING
    BOMID                   STRING NOT NULL,

    -- 8. COSTCALCULATIONMETHOD (INT)      → INT
    COSTCALCULATIONMETHOD   INT NOT NULL,

    -- 9. COSTMARKUP        (NUMERIC(32,16)) → DECIMAL(32,16)
    COSTMARKUP              DECIMAL(32,16) NOT NULL,

    -- 10. COSTPRICE      (NUMERIC(32,6))  → DECIMAL(32,6)
    COSTPRICE               DECIMAL(32,6) NOT NULL,

    -- 11. COMPANY         (NVARCHAR(4))   → STRING
    COMPANY                 STRING NOT NULL,

    -- 12. INVENTDIMID     (NVARCHAR(20))  → STRING
    INVENTDIMID             STRING NOT NULL,

    -- 13. ITEMID          (NVARCHAR(20))  → STRING
    ITEMID                  STRING NOT NULL,

    -- 14. QTY             (NUMERIC(32,6)) → DECIMAL(32,6)
    QTY                      DECIMAL(32,6) NOT NULL,

    -- 15. ROUTEID         (NVARCHAR(20))  → STRING
    ROUTEID                  STRING NOT NULL,

    -- 16. SALESMARKUP     (NUMERIC(32,6)) → DECIMAL(32,6)
    SALESMARKUP              DECIMAL(32,6) NOT NULL,

    -- 17. SALESPRICE      (NUMERIC(32,6)) → DECIMAL(32,6)
    SALESPRICE               DECIMAL(32,6) NOT NULL,

    -- 18. TRANSDATE       (DATETIME)      → TIMESTAMP
    TRANSDATE                TIMESTAMP NOT NULL,

    -- 19. UNITID          (NVARCHAR(10))  → STRING
    UNITID                   STRING NOT NULL,

    -- 20. PARTITION        (NVARCHAR(20)) → STRING
    PARTITION                STRING NOT NULL,

    -- 21. DATAAREAID       (NVARCHAR(4))   → STRING
    DATAAREAID               STRING NOT NULL,

    -- 22. SYNCSTARTDATETIME (DATETIME)   → TIMESTAMP
    SYNCSTARTDATETIME        TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Table migrated from dbo.SMRBIBOMCalcTableStaging – PK constraint removed (Delta does not support primary keys)'
""")

# COMMAND ----------

# ------------------------------------------------------------------
# 3️⃣  Optional: Verify creation
# ------------------------------------------------------------------
display(spark.sql(f"DESCRIBE DETAIL `dbe_dbx_internships`.`dbo`.`SMRBIBOMCalcTableStaging`"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 1: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 2446)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE TABLE `_placeholder_`.`_placeholder_`.`SMRBIBOMCalcTableStaging` (     -- 1. DEFINITIONGROUP  (NVARCHAR(60)) → STRING     DEFINITIONGROUP          STRING NOT NULL,      -- 2. EXECUTIONID       (NVARCHAR(90)) → STRING     EXECUTIONID             STRING NOT NULL,      -- 3. ISSELECTED        (INT)           → INT     ISSELECTED              INT NOT NULL,      -- 4. TRANSFERSTATUS    (INT)           → INT     TRANSFERSTATUS          INT NOT NULL,      -- 5. PRICECALCID       (NVARCHAR(20))  → STRING     PRICECALCID             STRING NOT NULL,      -- 6. BOMCALCTYPE       (INT)           → INT     BOMCALCTYPE             INT NOT NULL,      -- 7. BOMID             (NVARCHAR(20))  → STRING     BOMID                   STRING NOT NULL,      -- 8. COSTCALCULATIONMETHOD (INT)      → INT     COSTCALCULATIONMETHOD   INT NOT NULL,      -- 9. COSTMARKUP        (NUMERIC(32,16)) → DECIMAL(32,16)     COSTMARKUP              DECIMAL(32,16) NOT NULL,      -- 10. COSTPRICE      (NUMERIC(32,6))  → DECIMAL(32,6)     COSTPRICE               DECIMAL(32,6) NOT NULL,      -- 11. COMPANY         (NVARCHAR(4))   → STRING     COMPANY                 STRING NOT NULL,      -- 12. INVENTDIMID     (NVARCHAR(20))  → STRING     INVENTDIMID             STRING NOT NULL,      -- 13. ITEMID          (NVARCHAR(20))  → STRING     ITEMID                  STRING NOT NULL,      -- 14. QTY             (NUMERIC(32,6)) → DECIMAL(32,6)     QTY                      DECIMAL(32,6) NOT NULL,      -- 15. ROUTEID         (NVARCHAR(20))  → STRING     ROUTEID                  STRING NOT NULL,      -- 16. SALESMARKUP     (NUMERIC(32,6)) → DECIMAL(32,6)     SALESMARKUP              DECIMAL(32,6) NOT NULL,      -- 17. SALESPRICE      (NUMERIC(32,6)) → DECIMAL(32,6)     SALESPRICE               DECIMAL(32,6) NOT NULL,      -- 18. TRANSDATE       (DATETIME)      → TIMESTAMP     TRANSDATE                TIMESTAMP NOT NULL,      -- 19. UNITID          (NVARCHAR(10))  → STRING     UNITID                   STRING NOT NULL,      -- 20. PARTITION        (NVARCHAR(20)) → STRING     PARTITION                STRING NOT NULL,      -- 21. DATAAREAID       (NVARCHAR(4))   → STRING     DATAAREAID               STRING NOT NULL,      -- 22. SYNCSTARTDATETIME (DATETIME)   → TIMESTAMP     SYNCSTARTDATETIME        TIMESTAMP NOT NULL ) USING DELTA COMMENT 'Table migrated from dbo.SMRBIBOMCalcTableStaging – PK constraint removed (Delta does not support primary keys)'
# MAGIC ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
