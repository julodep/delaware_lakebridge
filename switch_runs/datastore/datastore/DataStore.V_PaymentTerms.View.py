# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_PaymentTerms.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_PaymentTerms.View.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create the V_PaymentTerms view in the target catalog and schema
# ------------------------------------------------------------------
# NOTE:
# * The T‑SQL settings `SET ANSI_NULLS ON` and `SET QUOTED_IDENTIFIER ON`
#   have no equivalent in Databricks; they are ignored.
# * The view is created using Spark SQL with fully‑qualified names
#   `dbe_dbx_internships`.`datastore`.`object_name`.
# * The T‑SQL `ISNULL` function is translated to Spark SQL `nvl`.
# * Backticks are used around identifiers to avoid clashes with reserved words.

# Drop the view if it already exists to allow safe recreation
spark.sql(f"""
DROP VIEW IF EXISTS `dbe_dbx_internships`.`datastore`.`V_PaymentTerms`;
""")

# COMMAND ----------

# Create (or replace) the view
spark.sql(f"""
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.`V_PaymentTerms` AS
SELECT
    upper(`dbe_dbx_internships`.`datastore`.`SMRBIPaymentTermStaging`.DataAreaId)   AS CompanyCode,
    upper(`dbe_dbx_internships`.`datastore`.`SMRBIPaymentTermStaging`.Name)     AS PaymentTermsCode,
    nvl(`dbe_dbx_internships`.`datastore`.`SMRBIPaymentTermStaging`.Description, '_N/A')
        AS PaymentTermsName
FROM `dbe_dbx_internships`.`datastore`.`SMRBIPaymentTermStaging`;
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
