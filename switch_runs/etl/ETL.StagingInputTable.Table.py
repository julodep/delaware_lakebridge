# Databricks notebook source
# MAGIC %md
# MAGIC # ETL.StagingInputTable.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/etl/etl_volume/etl/ETL.StagingInputTable.Table.sql`

# COMMAND ----------

# Create the table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS dbe_dbx_internships.ETL.StagingInputTable (
    SourceName        STRING NOT NULL,
    SourceSchema      STRING NOT NULL,
    SourceTable       STRING NOT NULL,
    TargetTable       STRING NOT NULL,
    PrimaryKey        STRING,
    WhereClause       STRING,
    DeltaWhereClause  STRING,
    Category          STRING,
    Status            STRING NOT NULL DEFAULT 'ACTIVE',
    Load              BOOLEAN NOT NULL,
    Rebuild           BOOLEAN NOT NULL,
    Description       STRING NOT NULL
)
USING delta
LOCATION '{catalog}/{schema}/StagingInputTable'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.dataSkippingNumIndexBatches' = '10'
)
""")

# COMMAND ----------

# Create primary key constraint
spark.sql(f"""
ALTER TABLE dbe_dbx_internships.ETL.StagingInputTable
ADD CONSTRAINT PK_StagingInputTable PRIMARY KEY (SourceName, SourceSchema, SourceTable)
""")

# COMMAND ----------

# Drop table and recreate with unique constraints in the initial CREATE TABLE statement
spark.sql(f"""
DROP TABLE IF EXISTS dbe_dbx_internships.ETL.StagingInputTable
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS dbe_dbx_internships.ETL.StagingInputTable (
    SourceName        STRING NOT NULL,
    SourceSchema      STRING NOT NULL,
    SourceTable       STRING NOT NULL,
    TargetTable       STRING NOT NULL,
    PrimaryKey        STRING,
    WhereClause       STRING,
    DeltaWhereClause  STRING,
    Category          STRING,
    Status            STRING NOT NULL DEFAULT 'ACTIVE',
    Load              BOOLEAN NOT NULL,
    Rebuild           BOOLEAN NOT NULL,
    Description       STRING NOT NULL,
    CONSTRAINT PK_StagingInputTable PRIMARY KEY (SourceName, SourceSchema, SourceTable),
    CONSTRAINT UC_StagingInputTable_SourceNameTargetTable UNIQUE (SourceName, TargetTable)
)
USING delta
LOCATION '{catalog}/{schema}/StagingInputTable'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.dataSkippingNumIndexBatches' = '10'
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 3: 
# MAGIC Only PRIMARY KEY and FOREIGN KEY constraints are currently supported.
# MAGIC == SQL (line 1, position 9) ==
# MAGIC EXPLAIN CREATE TABLE IF NOT EXISTS _placeholder_._placeholder_.StagingInputTable (     SourceName        STRING NOT NULL,     SourceSchema      STRING NOT NULL,     SourceTable       STRING NOT NULL,     TargetTable       STRING NOT NULL,     PrimaryKey        STRING,     WhereClause       STRING,     DeltaWhereClause  STRING,     Category          STRING,     Status            STRING NOT NULL DEFAULT 'ACTIVE',     Load              BOOLEAN NOT NULL,     Rebuild           BOOLEAN NOT NULL,     Description       STRING NOT NULL,     CONSTRAINT PK_StagingInputTable PRIMARY KEY (SourceName, SourceSchema, SourceTable),     CONSTRAINT UC_StagingInputTable_SourceNameTargetTable UNIQUE (SourceName, TargetTable) ) USING delta LOCATION '_placeholder_/_placeholder_/StagingInputTable' TBLPROPERTIES (     'delta.autoOptimize.optimizeWrite' = 'true',     'delta.dataSkippingNumIndexBatches' = '10' )
# MAGIC         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
# MAGIC
# MAGIC ```
