# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.Location.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.Location.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------
# NOTE: This notebook recreates the SQL Server table creation
#       shown in the legacy script:
#
#       SET ANSI_NULLS ON
#       SET QUOTED_IDENTIFIER ON
#       CREATE TABLE [DataStore].[Location] ( … )
#
#       • The ANSI and QUOTED_IDENTIFIER settings have no direct
#         equivalent in Databricks/Spark SQL, so they are omitted.
#       • Square‑bracket identifiers are removed – Spark uses
#         back‑ticks (``) for quoted identifiers.
#       • NVARCHAR column types are mapped to STRING; length limits
#         are not enforced by Spark SQL, but the semantics are the
#         same (variable‑length text).
#       • The table is created as a Delta Lake table so that it is
#         fully compatible with Unity Catalog.  Explicit NOT NULL
#         constraints are preserved.
# --------------------------------------------------------------

# Define the catalog and schema that the notebook should use.
# In a real environment these would be set to the desired Unity
# Catalog name (e.g., 'prod') and database (schema) name (e.g.,
# 'DataStore').  For this example we leave them as variables.
# Replace `catalog` and `schema` with actual names before running.
catalog = "my_catalog"   # <-- replace with your catalog name
schema = "DataStore"     # <-- replace with your schema name

# COMMAND ----------

# ---------------------------------------
# CREATE TABLE `catalog`.`schema`.`Location`
# ---------------------------------------

table_sql = f"""
CREATE TABLE `dbe_dbx_internships`.`datastore`.`Location` (
    LocationId        BIGINT    NOT NULL,
    LocationCode      STRING    NOT NULL,
    LocationName      STRING    NOT NULL,
    LocationCodeName  STRING    NOT NULL,
    DimensionName     STRING    NOT NULL
)
USING DELTA
"""

# COMMAND ----------

try:
    spark.sql(table_sql)
    print(f"Table dbe_dbx_internships.datastore.Location created successfully.")
except Exception as e:
    # If the table already exists, we simply notify the user
    if "ALREADY_EXISTS" in str(e):
        print(f"Table dbe_dbx_internships.datastore.Location already exists.")
    else:
        # Re‑raise unexpected errors
        raise

# COMMAND ----------

# --------------------------------------------------------------
# OPTIONAL: Verify the table schema (for debugging/testing)
# --------------------------------------------------------------
df = spark.table(f"dbe_dbx_internships.datastore.Location")
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
