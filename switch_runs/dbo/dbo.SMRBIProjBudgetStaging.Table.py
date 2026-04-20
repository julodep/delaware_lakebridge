# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIProjBudgetStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIProjBudgetStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
#  Create the table **SMRBIProjBudgetStaging** in the target catalog
#  and schema using the fully‑qualified notation:
#      dbe_dbx_internships.dbo.SMRBIProjBudgetStaging
#
#  Notes on the translation
#  -----------------------
#  • In T‑SQL the datatype *nvarchar(n)* maps to Spark SQL *STRING*.
#  • *datetime* maps to *TIMESTAMP*.
#  • The primary‑key definition in the original script is a clustered
#    key and is not supported in Delta Lake.  It is therefore omitted
#    from the DDL.  If you need uniqueness you can enforce it later
#    with a Spark “UNIQUE” constraint (available in Spark 3.2+) or
#    by adding a check in your ETL logic.
#  • All other table hint options (STATISTICS_NORECOMPUTE, IGNORE_DUP_KEY,
#    OPTIMIZE_FOR_SEQUENTIAL_KEY) are unsupported in Spark SQL and are
#    simply skipped.
#
#  In the following code we create the table using the default Delta
#  format.  If you want a specific storage location, add a
#  LOCATION clause to the CREATE TABLE statement.
# ------------------------------------------------------------------

# Drop the table first so that we can re‑create it unchanged.
spark.sql(
    f"""
    DROP TABLE IF EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIProjBudgetStaging`
    """
)

# COMMAND ----------

# Create the table with the translated column definitions.
spark.sql(
    f"""
    CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIProjBudgetStaging` (
        DEFINITIONGROUP STRING NOT NULL,
        EXECUTIONID STRING NOT NULL,
        ISSELECTED INT NOT NULL,
        TRANSFERSTATUS INT NOT NULL,
        BUDGETID STRING NOT NULL,
        BUDGETSTATE INT NOT NULL,
        BUDGETWORKFLOWSTATUS INT NOT NULL,
        DESCRIPTION STRING NOT NULL,
        ORIGINALBUDGETFORECASTMODEL STRING NOT NULL,
        REMAININGBUDGETFORECASTMODEL STRING NOT NULL,
        ROOTPROJID STRING NOT NULL,
        PARTITION STRING NOT NULL,
        DATAAREAID STRING NOT NULL,
        SYNCSTARTDATETIME TIMESTAMP NOT NULL,
        RECID BIGINT NOT NULL
        /* ---- Primary key and clustering options are omitted ----
           The original T‑SQL created a clustered PRIMARY KEY
           on (EXECUTIONID, BUDGETID, DATAAREAID, PARTITION).
           Delta Lake does not support explicit primary‑key
           or clustering in a CREATE TABLE statement.
        */
    )
    USING delta
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
