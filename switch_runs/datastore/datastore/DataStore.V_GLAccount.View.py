# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_GLAccount.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_GLAccount.View.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Create the V_GLAccount view in Databricks
# ------------------------------------------------------------------
# 1. Spark SQL uses back‑ticks for identifiers that contain special
#    characters or that clash with keywords.
# 2. All references are fully‑qualified with the target catalog
#    and schema (`dbe_dbx_internships` and `datastore` are placeholders that
#    will be replaced when the notebook is executed).
# 3. T‑SQL functions are mapped to their Spark equivalents:
#      ISNULL → IFNULL
#      CAST(... AS BIT) → CAST(... AS BOOLEAN)
#      FORMAT → date_format (not required here)
# 4. The `GO` batch terminator is omitted; instead a comment explains
#    the end of the CREATE VIEW block.
# ------------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.`V_GLAccount` AS
SELECT
    UPPER(LES.`Name`)                                                         AS CompanyCode,
    MAS.`MainAccountRecId`                                                   AS GLAccountId,
    MAS.`MainAccountId`                                                      AS GLAccountCode,
    IFNULL(MAS.`Name`, '_N/A')                                                AS GLAccountName,
    CAST(IFNULL(SM.`Name`, '_N/A') AS STRING)                                 AS GLAccountType,
    IFNULL(NULLIF(LCOAS.`ChartOfAccounts`, ''), '_N/A')                       AS ChartOfAccountsName,
    IFNULL(NULLIF(MACS.`MainAccountCategory`, ''), '_N/A')                  AS MainAccountCategory,
    IFNULL(NULLIF(MACS.`Description`, ''), '_N/A')                          AS MainAccountCategoryDescription,
    IFNULL(NULLIF(MACS.`MainAccountCategory`, ''), '_N/A') || ' ' ||
    IFNULL(NULLIF(MACS.`Description`, ''), '_N/A')                          AS MainAccountCategoryCodeDescription,
    IFNULL(NULLIF(MACS.`ReferenceId`, ''), 99999)                           AS MainAccountCategorySort,
    CAST(
        CASE WHEN MAS.`MAINACCOUNTID` LIKE '3%' THEN TRUE ELSE FALSE END
        AS BOOLEAN
    )                                                                         AS IsRevenueFlag,
    CAST(
        CASE
            WHEN MAS.`MAINACCOUNTID` LIKE '4%' OR MAS.`MAINACCOUNTID` LIKE '3%' THEN TRUE
            ELSE FALSE
        END
        AS BOOLEAN
    )                                                                         AS IsGrossProfitFlag
FROM `dbe_dbx_internships`.`datastore`.`SMRBIMainAccountStaging` MAS
INNER JOIN `dbe_dbx_internships`.`datastore`.`SMRBILedgerStaging` LES
    ON MAS.`ChartOfAccountsRecId` = LES.`ChartOfAccountsRecId`
LEFT JOIN (
    SELECT DISTINCT *
    FROM `dbe_dbx_internships`.`datastore`.`SMRBILedgerChartOfAccountsStaging`
) LCOAS
    ON LCOAS.`ChartOfAccountsRecId` = LES.`ChartOfAccountsRecId`
LEFT JOIN `dbe_dbx_internships`.`datastore`.`SMRBIMainAccountCategoryStaging` MACS
    ON MAS.`MainAccountCategory` = MACS.`MainAccountCategory`
LEFT JOIN `dbe_dbx_internships`.`datastore`.`StringMap` SM
    ON SM.`SourceSystem`  = 'D365FO'
   AND SM.`SourceTable`   = 'MainAccountStaging'
   AND SM.`SourceColumn`  = 'MainAccountType'
   AND SM.`Enum`          = MAS.`MainAccountType`;
""" )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
