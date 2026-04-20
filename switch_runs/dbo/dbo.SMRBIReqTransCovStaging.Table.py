# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIReqTransCovStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIReqTransCovStaging.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------------
# NOTE 1 –  Data‑type mapping
# --------------------------------------------------------------------
# T‑SQL data types are mapped to Spark/Delta Lake data types as follows:
#   NVARCHAR / VARCHAR          -> STRING
#   INT / INTEGER               -> INT
#   BIGINT                      -> BIGINT (Spark's BIGINT ↔ T‑SQL BIGINT)
#   NUMERIC(p,s) / DECIMAL(p,s) -> DECIMAL(p, s)
#   DATETIME                    -> TIMESTAMP
# The resulting table is a Delta Lake table in the specified
# catalog and schema.

# --------------------------------------------------------------------
# NOTE 2 –  Primary key / clustering
# --------------------------------------------------------------------
# The original script defines a composite PRIMARY KEY and a
# CLUSTERED index.  Delta Lake does not enforce primary‑key
# constraints or clustering, so we omit those constructs.
# If key semantics are required for downstream logic, add a
# commented flag or create a unique index manually if needed.

# --------------------------------------------------------------------
# 1. Resolve catalog and schema names
# --------------------------------------------------------------------
# If the variables are already set in the notebook, keep them.
# Otherwise, assign reasonable defaults.
if 'catalog' not in globals():
    # Default to the built‑in Spark catalog
    catalog = "spark_catalog"
if 'schema' not in globals():
    # Default to the default schema of the current catalog
    schema = "default"

# COMMAND ----------

# --------------------------------------------------------------------
# 2. Create the table
# --------------------------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIReqTransCovStaging` (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    COMPANY STRING NOT NULL,
    ISSUERECID BIGINT NOT NULL,
    QTY DECIMAL(32, 6) NOT NULL,
    RECEIPTRECID BIGINT NOT NULL,
    REQTRANSCOVRECID BIGINT NOT NULL,
    PARTITION STRING NOT NULL,
    DATAAREAID STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID BIGINT NOT NULL
)
-- Primary key is omitted – Delta Lake does not support it.
""")

# COMMAND ----------

# --------------------------------------------------------------------
# 3. Verify the schema was created correctly
# --------------------------------------------------------------------
df = spark.read.table(f"dbe_dbx_internships.dbo.SMRBIReqTransCovStaging")
print("Table created with schema:")
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
