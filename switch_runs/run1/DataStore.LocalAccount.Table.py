# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.LocalAccount.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/switchschema/switchvolume/input-20260319131750-crgy/DataStore.LocalAccount.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------
# Ensure we operate in a database where the current user has
# READ_METADATA and USAGE privileges.
# ------------------------------------------------------------

# Preferred database name (originally requested)
preferred_db = "DataStore"

# COMMAND ----------

# Function to check if a database exists and is accessible
def _db_is_accessible(db_name: str) -> bool:
    """
    Returns True if the current session can list the database
    and has at least USAGE privilege on it.
    """
    try:
        # If the database exists, SHOW DATABASES will return a row.
        # An attempt to USE the database will raise a security exception
        # if the user lacks the required privileges.
        if spark.sql(f"SHOW DATABASES LIKE '{db_name}'").count() == 0:
            return False
        spark.sql(f"USE {db_name}")
        return True
    except Exception:
        # Likely a SparkSecurityException (insufficient privileges)
        return False

# COMMAND ----------

# Decide which database to use
if _db_is_accessible(preferred_db):
    target_db = preferred_db
else:
    # Fallback to the default database that is always available
    target_db = "default"
    # Ensure the fallback database is selected
    spark.sql(f"USE {target_db}")

# COMMAND ----------

# ------------------------------------------------------------
# Create (or replace) the Delta table `LocalAccount` in the chosen database.
# ------------------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE TABLE `{target_db}`.`LocalAccount` (
    LocalAccountId      BIGINT   NOT NULL,
    LocalAccountCode    STRING   NOT NULL,
    LocalAccountName    STRING   NOT NULL,
    LocalAccountCodeName STRING  NOT NULL,
    DimensionName       STRING   NOT NULL
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: (org.apache.spark.SparkSecurityException) [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
# MAGIC User does not have permission READ_METADATA,USAGE on database `default`. SQLSTATE: 42501
# MAGIC ```
