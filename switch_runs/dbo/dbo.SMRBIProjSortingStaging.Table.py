# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBIProjSortingStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBIProjSortingStaging.Table.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# 1️⃣  Create the staging table in the target catalog and schema
# ------------------------------------------------------------------
# NOTE:
# • T‑SQL primary‑key / clustered‑index definitions are not supported in
#   Delta Lake.  They can be added later via ALTER TABLE constraints if
#   needed.
# • All identifiers are fully‑qualified: `dbe_dbx_internships.dbo.SMRBIProjSortingStaging`
# ------------------------------------------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS `dbe_dbx_internships`.`dbo`.`SMRBIProjSortingStaging` (
    DEFINITIONGROUP VARCHAR(60) NOT NULL,
    EXECUTIONID    VARCHAR(90) NOT NULL,
    ISSELECTED     INT         NOT NULL,
    TRANSFERSTATUS INT         NOT NULL,
    DESCRIPTION    VARCHAR(60) NOT NULL,
    SORTCODE       INT         NOT NULL,
    SORTINGID      VARCHAR(10) NOT NULL,
    PARTITION      VARCHAR(20) NOT NULL,
    DATAAREAID     VARCHAR(4)  NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL
);
""")

# COMMAND ----------

# ------------------------------------------------------------------
# 2️⃣  Example: Insert sample data (optional)
# ------------------------------------------------------------------
# Replace the values below with real data or use a DataFrame to
# populate the table later in your ETL pipeline.
# ------------------------------------------------------------------
spark.sql(f"""
INSERT INTO `dbe_dbx_internships`.`dbo`.`SMRBIProjSortingStaging` (
    DEFINITIONGROUP, EXECUTIONID, ISSELECTED, TRANSFERSTATUS,
    DESCRIPTION, SORTCODE, SORTINGID, PARTITION, DATAAREAID,
    SYNCSTARTDATETIME
) VALUES (
    'GroupA', 'Exec123', 1, 0,
    'Sample description', 10, 'SRT001', 'Part01', 'DA01',
    current_timestamp()
);
""")

# COMMAND ----------

# ------------------------------------------------------------------
# 3️⃣  Verify table creation (optional)
# ------------------------------------------------------------------
display(spark.table(f"dbe_dbx_internships.dbo.SMRBIProjSortingStaging"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
