# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIVendReviewCriterionRatingStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIVendReviewCriterionRatingStaging.Table.sql`

# COMMAND ----------

# -------------------------------------------------------------
# Create the staging table SMRBIVendReviewCriterionRatingStaging
# in the target Delta Lake catalog & schema.
# -------------------------------------------------------------

# Fully‑qualified name: dbe_dbx_internships.dbo.SMRBIVendReviewCriterionRatingStaging
table_name = f"`dbe_dbx_internships`.`dbo`.`SMRBIVendReviewCriterionRatingStaging`"

# COMMAND ----------

# Spark DataFrame schema mapping (T‑SQL → Spark SQL)
#   - NVARCHAR  → STRING
#   - INT       → INT
#   - BIGINT    → BIGINT
#   - DATETIME  → TIMESTAMP
#   - PRIMARY KEY is commented because Delta Lake does not enforce it natively.

create_table_sql = f"""
CREATE OR REPLACE TABLE {table_name} (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID   STRING NOT NULL,
    ISSELECTED    INT   NOT NULL,
    TRANSFERSTATUS INT  NOT NULL,
    CRITERION     BIGINT NOT NULL,
    RATING        INT   NOT NULL,
    VALIDFROM     TIMESTAMP NOT NULL,
    VALIDTO       TIMESTAMP NOT NULL,
    VENDORACCOUNTNUMBER STRING NOT NULL,
    VENDORDATAAREAID STRING NOT NULL,
    PARTITION     STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID         BIGINT NOT NULL
)
-- The original T‑SQL definition had a clustered PRIMARY KEY on 8 columns.
-- Delta Lake does not enforce primary keys, but you may add a UNIQUE
-- constraint or use a Delta Lake schema evolution rule if required.
"""

# COMMAND ----------

spark.sql(create_table_sql)

# COMMAND ----------

# -------------------------------------------------------------
# Optional: verify that the table was created
# -------------------------------------------------------------
spark.sql(f"DESCRIBE TABLE {table_name}").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
