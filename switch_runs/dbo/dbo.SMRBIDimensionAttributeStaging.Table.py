# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIDimensionAttributeStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIDimensionAttributeStaging.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------------
# Create the SMRBIDimensionAttributeStaging table in the target catalog
# and schema.
#
# All identifiers are fully‑qualified: dbe_dbx_internships.dbo.SMRBIDimensionAttributeStaging
# Data types have been mapped from T‑SQL to Spark SQL:
#   NVARCHAR   -> STRING
#   INT       -> INT
#   BIGINT    -> BIGINT
#   DATETIME  -> TIMESTAMP
# --------------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIDimensionAttributeStaging` (
    DEFINITIONGROUP            STRING  NOT NULL,
    EXECUTIONID                STRING  NOT NULL,
    ISSELECTED                 INT     NOT NULL,
    TRANSFERSTATUS             INT     NOT NULL,
    DIMENSIONNAME              STRING  NOT NULL,
    VIEWNAME                   STRING  NOT NULL,
    DIMENSIONATTRIBUTERECID    BIGINT  NOT NULL,
    BACKINGENTITYTABLENAME     STRING  NOT NULL,
    BACKINGENTITYVALUEFIELDNAME STRING NOT NULL,
    PARTITION                  STRING  NOT NULL,
    SYNCSTARTDATETIME          TIMESTAMP NOT NULL,
    RECID                      BIGINT  NOT NULL
);
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
