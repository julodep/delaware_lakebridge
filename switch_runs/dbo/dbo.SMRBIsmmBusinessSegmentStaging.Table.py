# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIsmmBusinessSegmentStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIsmmBusinessSegmentStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Drop the staging table if it already exists
# ------------------------------------------------------------------
spark.sql(f"DROP TABLE IF EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIsmmBusinessSegmentStaging`;")

# COMMAND ----------

# ------------------------------------------------------------------
# Create the staging table using Spark SQL data types
# ------------------------------------------------------------------
spark.sql(f"""
CREATE TABLE `dbe_dbx_internships`.`dbo`.`SMRBIsmmBusinessSegmentStaging`
(
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID STRING NOT NULL,
    ISSELECTED INT NOT NULL,
    TRANSFERSTATUS INT NOT NULL,
    SEGMENTDESCRIPTION STRING NOT NULL,
    SEGMENTCODE STRING NOT NULL,
    `PARTITION` STRING NOT NULL,
    DATAAREAID STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL
)
USING DELTA
""")

# COMMAND ----------

# ------------------------------------------------------------------
# Document the intended primary key (Databricks does not enforce it)
# ------------------------------------------------------------------
spark.sql(f"""
COMMENT ON TABLE `dbe_dbx_internships`.`dbo`.`SMRBIsmmBusinessSegmentStaging`
IS 'Intended primary key: (EXECUTIONID, SEGMENTCODE, DATAAREAID, PARTITION)';
""")

# COMMAND ----------

# ------------------------------------------------------------------
# Verify the table creation by describing its schema
# ------------------------------------------------------------------
display(spark.sql(f"DESCRIBE `dbe_dbx_internships`.`dbo`.`SMRBIsmmBusinessSegmentStaging`"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
