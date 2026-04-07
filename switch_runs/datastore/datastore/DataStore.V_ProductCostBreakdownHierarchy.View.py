# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_ProductCostBreakdownHierarchy.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_ProductCostBreakdownHierarchy.View.sql`

# COMMAND ----------

# Databricks (Unity Catalog) view creation – Spark SQL

# Assumes `catalog` and `schema` variables are defined in the notebook context.
# Example:
# catalog = "my_catalog"
# schema  = "my_schema"

spark.sql(f"""
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.V_ProductCostBreakdownHierarchy
AS
SELECT DISTINCT
    CompanyCode,
    Level_1,
    Level_2,
    Level_3
FROM (
    -- 1. Source rows from CostSheetNodeHierarchyTable
    SELECT
        -- Company code is the upper‑case DataAreaId
        UPPER(DataAreaId) AS CompanyCode,

        -- Build Level_1 with logic:
        --   If Level_1_Code is NULL or empty, use '_N/A'
        --   Append ' | ' + Level_1_Description if the description is non‑empty
        COALESCE(
            NULLIF(CSNHT.Level_1_Code, ''),
            '_N/A'
        ) || IF(length(CSNHT.Level_1_Description) > 0,
                ' | ' || CSNHT.Level_1_Description,
                ''
        ) AS Level_1,

        -- Same pattern for Level_2
        COALESCE(
            NULLIF(CSNHT.Level_2_Code, ''),
            '_N/A'
        ) || IF(length(CSNHT.Level_2_Description) > 0,
                ' | ' || CSNHT.Level_2_Description,
                ''
        ) AS Level_2,

        -- Same pattern for Level_3
        COALESCE(
            NULLIF(CSNHT.Level_3_Code, ''),
            '_N/A'
        ) || IF(length(CSNHT.Level_3_Description) > 0,
                ' | ' || CSNHT.Level_3_Description,
                ''
        ) AS Level_3

    FROM `dbe_dbx_internships`.`datastore`.CostSheetNodeHierarchyTable CSNHT

    UNION ALL

    -- 2. Additional rows from SMRBIOfficeAddinLegalEntityStaging
    SELECT
        UPPER(CompanyId)          AS CompanyCode,
        '_N/A'                    AS Level_1,
        '_N/A'                    AS Level_2,
        '_N/A'                    AS Level_3

    FROM `dbe_dbx_internships`.`datastore`.SMRBIOfficeAddinLegalEntityStaging
) AS sub
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 1714)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE VIEW `_placeholder_`.`_placeholder_`.V_ProductCostBreakdownHierarchy AS SELECT DISTINCT     CompanyCode,     Level_1,     Level_2,     Level_3 FROM (     -- 1. Source rows from CostSheetNodeHierarchyTable     SELECT         -- Company code is the upper‑case DataAreaId         UPPER(DataAreaId) AS CompanyCode,          -- Build Level_1 with logic:         --   If Level_1_Code is NULL or empty, use '_N/A'         --   Append ' | ' + Level_1_Description if the description is non‑empty         COALESCE(             NULLIF(CSNHT.Level_1_Code, ''),             '_N/A'         ) || IF(length(CSNHT.Level_1_Description) > 0,                 ' | ' || CSNHT.Level_1_Description,                 ''         ) AS Level_1,          -- Same pattern for Level_2         COALESCE(             NULLIF(CSNHT.Level_2_Code, ''),             '_N/A'         ) || IF(length(CSNHT.Level_2_Description) > 0,                 ' | ' || CSNHT.Level_2_Description,                 ''         ) AS Level_2,          -- Same pattern for Level_3         COALESCE(             NULLIF(CSNHT.Level_3_Code, ''),             '_N/A'         ) || IF(length(CSNHT.Level_3_Description) > 0,                 ' | ' || CSNHT.Level_3_Description,                 ''         ) AS Level_3      FROM `_placeholder_`.`_placeholder_`.CostSheetNodeHierarchyTable CSNHT      UNION ALL      -- 2. Additional rows from SMRBIOfficeAddinLegalEntityStaging     SELECT         UPPER(CompanyId)          AS CompanyCode,         '_N/A'                    AS Level_1,         '_N/A'                    AS Level_2,         '_N/A'                    AS Level_3      FROM `_placeholder_`.`_placeholder_`.SMRBIOfficeAddinLegalEntityStaging ) AS sub
# MAGIC ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
