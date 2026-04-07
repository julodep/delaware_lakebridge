# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_Employee.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_Employee.View.sql`

# COMMAND ----------

# -------------------------------------------------
# NOTE: The original T‑SQL created a schema‑qualified
# view named [DataStore].[V_Employee] with a SELECT that
# performed NULL handling and string concatenation.
#
# In Databricks/Unity Catalog the view is persisted as:
#     dbe_dbx_internships.datastore.V_Employee
# -------------------------------------------------

# ----  Define the target catalog and schema  ----
catalog = "your_catalog_name"   # <-- replace with your catalog
schema  = "your_schema_name"    # <-- replace with your schema

# COMMAND ----------

# ----  Fully‑qualified view name (using backticks for safety)  ----
view_name = f"`dbe_dbx_internships`.`datastore`.`V_Employee`"

# COMMAND ----------

# ----  Build and execute the view definition  ----
spark.sql(f"""
CREATE OR REPLACE VIEW {view_name} AS
SELECT
    -- Unique identifier for the employee
    HCMWorkerRecId          AS EmployeeId,

    -- Employee code (personnel number)
    PersonnelNumber         AS EmployeeCode,

    -- Employee name – replace empty string with '_N/A'
    COALESCE(NULLIF(Name, ''), '_N/A')          AS EmployeeName,

    -- Concatenate personnel number and name, handling NULLs
    concat_ws(' ',
              PersonnelNumber,
              COALESCE(NULLIF(Name, ''), '_N/A')
             )                                       AS EmployeeCodeName,

    -- Constant dimension name
    'Werknemer'            AS DimensionName
FROM `dbe_dbx_internships`.`datastore`.`SMRBIHcmWorkerStaging`;
""")

# COMMAND ----------

# ----  View is now persistent in Unity catalog  ----
#     dbe_dbx_internships.datastore.V_Employee

# ----  To test/query the new view  ----
df = spark.table(f"dbe_dbx_internships.datastore.V_Employee")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 765)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE VIEW `your_catalog_name`.`your_schema_name`.`V_Employee` AS SELECT     -- Unique identifier for the employee     HCMWorkerRecId          AS EmployeeId,      -- Employee code (personnel number)     PersonnelNumber         AS EmployeeCode,      -- Employee name – replace empty string with '_N/A'     COALESCE(NULLIF(Name, ''), '_N/A')          AS EmployeeName,      -- Concatenate personnel number and name, handling NULLs     concat_ws(' ',               PersonnelNumber,               COALESCE(NULLIF(Name, ''), '_N/A')              )                                       AS EmployeeCodeName,      -- Constant dimension name     'Werknemer'            AS DimensionName FROM `your_catalog_name`.`your_schema_name`.`SMRBIHcmWorkerStaging`;
# MAGIC ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
