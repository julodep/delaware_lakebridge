# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_DeliveryTerms.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_DeliveryTerms.View.sql`

# COMMAND ----------

# -------------------------------------------------------------
# View   : DataStore.V_DeliveryTerms
# -------------------------------------------------------------
#
# The original T‑SQL code created a view with the following
# definition:
#
#     CREATE VIEW [DataStore].[V_DeliveryTerms] AS
#         SELECT UPPER(DTS.DataAreaId)        AS CompanyCode ,
#                UPPER(DTS.TermsCode)         AS DeliveryTermsCode ,
#                ISNULL(DTS.TermsDescription,'_N/A') AS DeliveryTermsName
#         FROM dbo.SMRBIDeliveryTermsStaging DTS
#
# T‑SQL specific statements such as
#     SET ANSI_NULLS ON
#     SET QUOTED_IDENTIFIER ON
# and the “GO” batch separators are ignored in Databricks – they
# have no equivalent in Spark SQL – so we simply comment them out
# for completeness.
#
# In Spark SQL we:
#     • Use `upper()` for `UPPER()`.
#     • Replace `ISNULL()` with `coalesce()` (the equivalent of COALESCE in
#       Spark, along with a literal default value).
#     • Fully‑qualify all object references using the provided `dbe_dbx_internships`
#       and `datastore` placeholders.  The view itself lives in the `DataStore`
#       schema of the same catalog, so the view name is
#       `dbe_dbx_internships`.`DataStore`.`V_DeliveryTerms`.
# -------------------------------------------------------------

# Commented out T‑SQL batch / SET statements – they are no‑ops in Spark
# SET ANSI_NULLS ON
# SET QUOTED_IDENTIFIER ON
# GO

# Create or replace the view in the `DataStore` schema
spark.sql(f"""
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`DataStore`.`V_DeliveryTerms` AS
SELECT
    upper(DTS.DataAreaId)                                     AS CompanyCode,
    upper(DTS.TermsCode)                                      AS DeliveryTermsCode,
    coalesce(DTS.TermsDescription, lit('_N/A'))               AS DeliveryTermsName
FROM `dbe_dbx_internships`.`datastore`.`SMRBIDeliveryTermsStaging` AS DTS
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
