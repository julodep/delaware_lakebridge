# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIExchangeRateStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIExchangeRateStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
#  Create the Delta table ``SMRBIExchangeRateStaging`` in the target
#  catalog/scheme.  All catalog and schema references are fully‑qualified
#  in the form `dbe_dbx_internships`.`dbo`.  The table definition mirrors the
#  original T‑SQL schema as closely as possible:
#     - nvarchar   → STRING
#     - int        → INT
#     - bigint     → BIGINT
#     - datetime   → TIMESTAMP
#     - numeric(p,s) → DECIMAL(p,s)
#  Delta Lake does not support PRIMARY KEY constraints directly.  The
#  primary key defined in the T‑SQL source is therefore omitted; you can
#  enforce uniqueness at the application layer or using a separate
#  unique index if required.
# ------------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIExchangeRateStaging` (
    DEFINITIONGROUP            STRING  NOT NULL,
    EXECUTIONID                STRING  NOT NULL,
    ISSELECTED                 INT     NOT NULL,
    TRANSFERSTATUS             INT     NOT NULL,
    STARTDATE                  TIMESTAMP NOT NULL,
    ENDDATE                    TIMESTAMP NOT NULL,
    FROMCURRENCY               STRING  NOT NULL,
    TOCURRENCY                 STRING  NOT NULL,
    RATETYPEDESCRIPTION        STRING  NOT NULL,
    RATETYPENAME               STRING  NOT NULL,
    RATE                       DECIMAL(32,16) NOT NULL,
    EXCHANGERATERECID          BIGINT NOT NULL,
    EXCHANGERATE               DECIMAL(32,16) NOT NULL,
    PARTITION                  STRING  NOT NULL,
    SYNCSTARTDATETIME          TIMESTAMP NOT NULL,
    RECID                      BIGINT  NOT NULL
)
-- NOTE: Delta Lake does not support a native PRIMARY KEY.  If you need
-- uniqueness guarantees, consider adding a unique index manually or
-- validating before inserts.
""")

# COMMAND ----------

# ------------------------------------------------------------------
#  Optional: Verify that the table was created successfully by
#  describing its schema.  (This step can be omitted in production.)
# ------------------------------------------------------------------
spark.sql(f"DESC DETAIL `dbe_dbx_internships`.`dbo`.`SMRBIExchangeRateStaging`").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
