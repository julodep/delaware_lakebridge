# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_BusinessSegment.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_BusinessSegment.View.sql`

# COMMAND ----------

# --------------------------------------------------------------------
# Create a persistent view V_BusinessSegment in Unity Catalog.
# We translate the T‑SQL CREATE VIEW statement into a Databricks
# spark.sql invocation.  All catalog and schema names are fully‑qualified
# with the placeholders dbe_dbx_internships and datastore.
# --------------------------------------------------------------------

# NOTE: T‑SQL “SET ANSI_NULLS ON” and “SET QUOTED_IDENTIFIER ON” are SQL Server
# session settings that have no direct analogue in Spark SQL.  They can be
# safely omitted in Databricks.

spark.sql(f"""
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.V_BusinessSegment AS
SELECT DISTINCT
    -- Identifier of the business segment
    DFTS.FinancialTagRecId                                 AS BusinessSegmentId,

    -- Code and description columns (originally quoted in T‑SQL)
    DFTS.`Value`                                            AS BusinessSegmentCode,
    DFTS.`Description`                                      AS BusinessSegmentName,

    -- Concatenated code + description for display purposes.  Spark requires
    -- the concat_ functions – here we use concat_ws to handle potential nulls.
    concat_ws(' ', DFTS.`Value`, DFTS.`Description`)         AS BusinessSegmentCodeName,

    -- The dimension name is aliased with “AS” (the T‑SQL syntax “DimensionName = …”
    -- is replaced by the standard Spark alias operator).
    DAS.DimensionName                                      AS DimensionName

FROM   `dbe_dbx_internships`.`datastore`.SMRBIDimensionFinancialTagStaging   AS DFTS
JOIN   `dbe_dbx_internships`.`datastore`.SMRBIDimensionAttributeDirCategoryStaging AS DADCS
          ON DFTS.FinancialTagCategory = DADCS.DirCategory
JOIN   `dbe_dbx_internships`.`datastore`.SMRBIDimensionAttributeStaging        AS DAS
          ON DADCS.DimensionAttribute = DAS.DimensionAttributeRecId

WHERE  DAS.DimensionName = 'Business_segment';
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
