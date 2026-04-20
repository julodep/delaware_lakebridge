# Databricks notebook source
# MAGIC %md
# MAGIC # dbo.SMRBILogisticsAddressCountryRegionTranslationStaging.Table
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/dbo/dbo_volume/dbo/dbo.SMRBILogisticsAddressCountryRegionTranslationStaging.Table.sql`

# COMMAND ----------

# --------------------------------------------------------------------------
# Create the staging table in the target catalog and schema.
#  - All column names and data types are mapped from the original T‑SQL
#    definition to Spark SQL data types.
#  - PRIMARY KEY constraints are not supported in Delta Lake; they are
#    commented out with an explanation. If you require uniqueness you can
#    enforce it at the application level or use a unique index in a
#    downstream process.
# --------------------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE `dbe_dbx_internships`.`dbo`.`SMRBILogisticsAddressCountryRegionTranslationStaging` USING DELTA (
    DEFINITIONGROUP STRING NOT NULL,
    EXECUTIONID   STRING NOT NULL,
    ISSELECTED    INT   NOT NULL,
    TRANSFERSTATUS INT  NOT NULL,
    COUNTRYREGIONID STRING NOT NULL,
    LANGUAGEID STRING NOT NULL,
    SHORTNAME STRING NOT NULL,
    COUNTRYREGIONRECID BIGINT NOT NULL,
    PARTITION STRING NOT NULL,
    SYNCSTARTDATETIME TIMESTAMP NOT NULL,
    RECID BIGINT NOT NULL
);
-- PRIMARY KEY is intentionally omitted: Delta Lake does not enforce
-- PRIMARY KEY constraints. If a uniqueness guarantee is needed,
-- consider adding a unique index in a downstream process or
-- handling it via application logic.
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near 'DEFINITIONGROUP'. SQLSTATE: 42601 (line 1, pos 137)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE TABLE `_placeholder_`.`_placeholder_`.`SMRBILogisticsAddressCountryRegionTranslationStaging` USING DELTA (     DEFINITIONGROUP STRING NOT NULL,     EXECUTIONID   STRING NOT NULL,     ISSELECTED    INT   NOT NULL,     TRANSFERSTATUS INT  NOT NULL,     COUNTRYREGIONID STRING NOT NULL,     LANGUAGEID STRING NOT NULL,     SHORTNAME STRING NOT NULL,     COUNTRYREGIONRECID BIGINT NOT NULL,     PARTITION STRING NOT NULL,     SYNCSTARTDATETIME TIMESTAMP NOT NULL,     RECID BIGINT NOT NULL ); -- PRIMARY KEY is intentionally omitted: Delta Lake does not enforce -- PRIMARY KEY constraints. If a uniqueness guarantee is needed, -- consider adding a unique index in a downstream process or -- handling it via application logic.
# MAGIC -----------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
