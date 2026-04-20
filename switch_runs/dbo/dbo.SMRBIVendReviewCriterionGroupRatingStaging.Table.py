# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIVendReviewCriterionGroupRatingStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIVendReviewCriterionGroupRatingStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create a persistent staging table that mirrors the T‑SQL definition
# ------------------------------------------------------------------
# NOTE:  Delta Lake does not provide native primary‑key enforcement.
# The “PRIMARY KEY CLUSTERED” clause is retained only for documentation
# purposes and will be ignored by Databricks.  If you need unique
# constraints, consider adding a separate table‑level constraint or
# using a `CHECK` TABLE option after table creation.
# ------------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIVendReviewCriterionGroupRatingStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    AVERAGERATING DECIMAL(32,6) NOT NULL,
    CRITERIONGROUP BIGINT NOT NULL,
    VENDORACCOUNTNUMBER STRING NOT NULL,
    VENDORDATAAREAID STRING NOT NULL,
    `PARTITION` STRING NOT NULL,              -- quoted to avoid reserved keyword conflict
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID BIGINT NOT NULL
    -- Primary‑key columns (for reference only – not enforced)
    -- PRIMARY KEY (EXECUTIONID, CRITERIONGROUP, VENDORACCOUNTNUMBER,
    --              VENDORDATAAREAID, `PARTITION`)
);
""")

# COMMAND ----------

# ------------------------------------------------------------------
# Verify that the table was created with the expected schema
# ------------------------------------------------------------------
spark.sql(f"DESCRIBE `dbe_dbx_internships`.`dbo`.`SMRBIVendReviewCriterionGroupRatingStaging`").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 745)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `_placeholder_`.`_placeholder_`.`SMRBIVendReviewCriterionGroupRatingStaging` (     DEFINITIONGROUP STRING NOT NULL,     EXECUTIONID STRING NOT NULL,     ISSELECTED INT NOT NULL,     TRANSFERSTATUS INT NOT NULL,     AVERAGERATING DECIMAL(32,6) NOT NULL,     CRITERIONGROUP BIGINT NOT NULL,     VENDORACCOUNTNUMBER STRING NOT NULL,     VENDORDATAAREAID STRING NOT NULL,     `PARTITION` STRING NOT NULL,              -- quoted to avoid reserved keyword conflict     SYNCSTARTDATETIME TIMESTAMP NOT NULL,     RECID BIGINT NOT NULL     -- Primary‑key columns (for reference only – not enforced)     -- PRIMARY KEY (EXECUTIONID, CRITERIONGROUP, VENDORACCOUNTNUMBER,     --              VENDORDATAAREAID, `PARTITION`) );
# MAGIC -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
