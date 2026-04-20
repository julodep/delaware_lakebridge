# Databricks notebook source
# MAGIC %md
# MAGIC # ETL.StagingInputTable.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/etl/etl_volume/etl/ETL.StagingInputTable.Table.sql`

# COMMAND ----------

# Create table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS `catalog`.`schema`.`StagingInputTable` (
    `SourceName` STRING NOT NULL,
    `SourceSchema` STRING NOT NULL,
    `SourceTable` STRING NOT NULL,
    `TargetTable` STRING NOT NULL,
    `PrimaryKey` STRING,
    `WhereClause` STRING,
    `DeltaWhereClause` STRING,
    `Category` STRING,
    `Status` STRING NOT NULL DEFAULT 'ACTIVE',
    `Load` BOOLEAN NOT NULL,
    `Rebuild` BOOLEAN NOT NULL,
    `Description` STRING NOT NULL
) USING delta
""")

# COMMAND ----------

# Create primary key constraint
spark.sql(f"""
ALTER TABLE `catalog`.`schema`.`StagingInputTable`
ADD CONSTRAINT `PK_catalog_schema_StagingInputTable` PRIMARY KEY (`SourceName`, `SourceSchema`, `SourceTable`)
""")

# COMMAND ----------

# Drop unique constraint, as it is not directly supported in Databricks Delta Lake tables
# Instead, you can create a CHECK constraint or handle uniqueness in your application logic
#spark.sql(f"""
#ALTER TABLE `catalog`.`schema`.`StagingInputTable`
#ADD CONSTRAINT `UC_catalog_schema_StagingInputTable_SourceNameTargetTable` UNIQUE (`SourceName`, `TargetTable`)
#""")

# Alternatively, to enforce uniqueness, you can use CHECK constraint
spark.sql(f"""
ALTER TABLE `catalog`.`schema`.`StagingInputTable`
ADD CONSTRAINT `CK_catalog_schema_StagingInputTable_SourceNameTargetTable` CHECK (
    `SourceName` IS NOT NULL AND `TargetTable` IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM `catalog`.`schema`.`StagingInputTable` t2
        WHERE t2.`SourceName` = `SourceName` AND t2.`TargetTable` = `TargetTable`
        AND t2.`SourceName` != `SourceName` OR t2.`TargetTable` != `TargetTable`
    )
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
