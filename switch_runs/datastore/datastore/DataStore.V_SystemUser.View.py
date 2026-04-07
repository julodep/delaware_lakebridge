# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_SystemUser.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_SystemUser.View.sql`

# COMMAND ----------

# ------------------------------------------------------------------
#  Databricks notebook – Transpiled definition for the view
#  -------------------------------------------------------
#  This script creates the view `V_SystemUser` inside the target
#  Unity Catalog schema.  All table references are fully‑qualified
#  using the `dbe_dbx_internships` and `datastore` placeholders supplied by the
#  environment.
#
#  T‑SQL constructs that are irrelevant in Spark (e.g. SET ANSI_NULLS,
#  SET QUOTED_IDENTIFIER, GO batch separators) have been removed.
# ------------------------------------------------------------------

# Create (or replace) the view
spark.sql(f"""
   CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.`V_SystemUser` AS
     SELECT
       -- SystemUserCode:  upper‑case of DPS.User_, defaulting to '_N/A'
       coalesce(upper(DPS.User_), '_N/A')                      AS SystemUserCode,

       -- UserName:  SUS.UserName, defaulting to '_N/A'
       coalesce(SUS.UserName, '_N/A')                          AS UserName,

       -- DomainUserName:  SUS.NetworkDomain + '\' + SUS.Alias.
       --  If the CONCAT result is NULL or empty, it defaults to '_N/A'.
       coalesce(nullif(concat(sus.NetworkDomain, '\\', sus.Alias), ''), '_N/A')
                                                                 AS DomainUserName

     FROM   `dbe_dbx_internships`.`datastore`.`SMRBIHcmWorkerStaging`  AS HWS
     JOIN   `dbe_dbx_internships`.`datastore`.`SMRBIDirPersonStaging` AS DPS
            ON HWS.HcmWorkerRecId = DPS.DirPersonRecId
     JOIN   `dbe_dbx_internships`.`datastore`.`SMRBISystemUserStaging` AS SUS
            ON DPS.User_ = SUS.UserId
""")

# COMMAND ----------

# Verify that the view exists (optional – helpful for debugging)
spark.sql(f"DESC VIEW `dbe_dbx_internships`.`datastore`.`V_SystemUser`").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 1000)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE VIEW `_placeholder_`.`_placeholder_`.`V_SystemUser` AS      SELECT        -- SystemUserCode:  upper‑case of DPS.User_, defaulting to '_N/A'        coalesce(upper(DPS.User_), '_N/A')                      AS SystemUserCode,         -- UserName:  SUS.UserName, defaulting to '_N/A'        coalesce(SUS.UserName, '_N/A')                          AS UserName,         -- DomainUserName:  SUS.NetworkDomain + '' + SUS.Alias.        --  If the CONCAT result is NULL or empty, it defaults to '_N/A'.        coalesce(nullif(concat(sus.NetworkDomain, '\', sus.Alias), ''), '_N/A')                                                                  AS DomainUserName       FROM   `_placeholder_`.`_placeholder_`.`SMRBIHcmWorkerStaging`  AS HWS      JOIN   `_placeholder_`.`_placeholder_`.`SMRBIDirPersonStaging` AS DPS             ON HWS.HcmWorkerRecId = DPS.DirPersonRecId      JOIN   `_placeholder_`.`_placeholder_`.`SMRBISystemUserStaging` AS SUS             ON DPS.User_ = SUS.UserId
# MAGIC ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
