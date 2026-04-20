# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIForecastModelStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIForecastModelStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
#  Databricks notebook – create the SMRBIForecastModelStaging table
#
#  The original T‑SQL statement defines a table that is later used as a
#  staging area for forecast models.  All references are fully qualified
#  with the dbe_dbx_internships and dbo placeholders that the target runtime
#  will replace with the actual names (e.g., `my_catalog`.`etl`).
#
#  Note:
#   * Delta Lake (the engine used by Databricks) does **not** support
#     declarative primary‑key constraints.  The PRIMARY KEY clause in
#     the T‑SQL definition is therefore omitted in the creation
#     statement.  If the application needs to enforce uniqueness, it
#     can be handled in downstream logic or by adding a unique index
#     manually in a post‑process step.
#
#   * SQL data‑type mappings:
#         NVARCHAR(n)  → STRING
#         INT          → INT
#         DATETIME     → TIMESTAMP
#
#   * The table is created as a Delta table by default in Databricks.
#     The `CREATE TABLE IF NOT EXISTS` syntax ensures that running the
#     notebook multiple times does not produce an error.
# ------------------------------------------------------------------

# Drop any existing staging table so that this run creates a fresh table.
spark.sql(f"DROP TABLE IF EXISTS `dbe_dbx_internships`.`dbo`.SMRBIForecastModelStaging")

# COMMAND ----------

# Create the staging table with column definitions that match the original
# T‑SQL schema.  All columns are marked NOT NULL to reflect the original
# definition.  No PRIMARY KEY is declared because Delta Lake does not
# enforce it.
spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.SMRBIForecastModelStaging (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID   STRING NOT NULL,
    ISSELECTED    INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    MODELID       STRING NOT NULL,
    MODELNAME     STRING NOT NULL,
    TYPE          INT NOT NULL,
    COMPANY       STRING NOT NULL,
    PARTITION     STRING NOT NULL,
    DATAAREAID    STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL
)
""")

# COMMAND ----------

# ------------------------------------------------------------------
#  Optional: Verify that the table exists and inspect its schema.
# ------------------------------------------------------------------
df_schema = spark.table(f"dbe_dbx_internships.dbo.SMRBIForecastModelStaging").printSchema()
print("Table created successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
