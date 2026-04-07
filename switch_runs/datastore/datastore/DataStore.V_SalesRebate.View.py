# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_SalesRebate.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_SalesRebate.View.sql`

# COMMAND ----------

# -------------------------------------------------------------
# Databricks view creation – V_SalesRebate
# -------------------------------------------------------------
# All object names are fully-qualified:
#   `dbe_dbx_internships`.`datastore`.<object_name>
#
# Function mapping (T-SQL → Spark SQL)
#   ISNULL              → COALESCE
#   NULLIF              → NULLIF
#   UPPER, LTRIM, RTRIM → UPPER, TRIM
#   LEFT(sql, n)        → SUBSTRING(sql, 1, n)
#   CHARINDEX(sub, str) → INSTR(str, sub)
#   CASE WHEN … THEN … ELSE … END → CASE … END HIDDEN / WHEN
# -------------------------------------------------------------

sql_create_view = f"""
-- Create or replace the persistent view in Unity Catalog
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.`V_SalesRebate` AS
SELECT
    UPPER(S.DataAreaId)                            AS CompanyCode,
    S.PdsRebateId                                 AS SalesRebateCode,
    S.SalesInvoiceId                              AS SalesInvoiceCode,
    S.CustInvoiceTransRefRecId                    AS SalesInvoiceLineId,
    -- ProductCode: if ItemId is NULL or empty → '_N/A'
    IFNULL(NULLIF(UPPER(S.ItemId), ''), '_N/A')    AS ProductCode,
    S.CustAccount                                  AS RebateCustomerCode,
    S.CurrencyCode                                AS RebateCurrencyCode,
    S.PdsStartingRebateAmt                        AS RebateAmountOriginal,

    -- Total of corrected rebates that are COMPLETED (status 5)
    SUM(COALESCE(S1.PdsCorrectedRebateAmt, 0))    AS RebateAmountCompleted,

    -- Total of corrected rebates that are MARKED (status 6)
    SUM(COALESCE(S2.PdsCorrectedRebateAmt, 0))    AS RebateAmountMarked,

    -- Total of corrected rebates that are CANCELLED (status 9)
    SUM(COALESCE(S3.PdsCorrectedRebateAmt, 0))    AS RebateAmountCancelled,

    -- Net variance compared to the original amount
    CASE
        WHEN COALESCE(S1.PdsCorrectedRebateAmt, 0) = 0 THEN 0
        ELSE SUM(COALESCE(S1.PdsCorrectedRebateAmt, 0) - S.PdsStartingRebateAmt)
    END                                            AS RebateAmountVariance
FROM
    -- -----------------------------------------------------------------
    -- 1. Deduplicate staging rows – Normalised PdsRebateId
    -- -----------------------------------------------------------------
    (
        SELECT DISTINCT
            CASE
                WHEN S.PdsRebateId LIKE '%/C%' THEN
                    TRIM(SUBSTRING(S.PdsRebateId, 1, INSTR(S.PdsRebateId, '/C') - 1))
                ELSE S.PdsRebateId
            END                                AS PdsRebateId,
            S.CurrencyCode,
            S.CustAccount,
            S.DataAreaId,
            S.ItemId,
            S.SalesInvoiceId,
            S.CustInvoiceTransRefRecId,
            S.PdsStartingRebateAmt
        FROM `dbe_dbx_internships`.`datastore`.`SMRBIPdsRebateTableStaging` S
    ) S
    -- -----------------------------------------------------------------
    -- 2. Left-join with status 5, 6 and 9 rows (the same normalisation
    --    logic is applied in the ON clause)
    -- -----------------------------------------------------------------
    LEFT JOIN `dbe_dbx_internships`.`datastore`.`SMRBIPdsRebateTableStaging` S1
        ON S.PdsRebateId =
                CASE
                    WHEN S1.PdsRebateId LIKE '%/C%' THEN
                        TRIM(SUBSTRING(S1.PdsRebateId, 1, INSTR(S1.PdsRebateId, '/C') - 1))
                    ELSE S1.PdsRebateId
                END
        AND S.DataAreaId = S1.DataAreaId
        AND S.SalesInvoiceId = S1.SalesInvoiceId
        AND S.ItemId = S1.ItemId
        AND S.CustInvoiceTransRefRecId = S1.CustInvoiceTransRefRecId
        AND S1.PdsRebateStatus = 5

    LEFT JOIN `dbe_dbx_internships`.`datastore`.`SMRBIPdsRebateTableStaging` S2
        ON S.PdsRebateId =
                CASE
                    WHEN S2.PdsRebateId LIKE '%/C%' THEN
                        TRIM(SUBSTRING(S2.PdsRebateId, 1, INSTR(S2.PdsRebateId, '/C') - 1))
                    ELSE S2.PdsRebateId
                END
        AND S.DataAreaId = S2.DataAreaId
        AND S.SalesInvoiceId = S2.SalesInvoiceId
        AND S.ItemId = S2.ItemId
        AND S.CustInvoiceTransRefRecId = S2.CustInvoiceTransRefRecId
        AND S2.PdsRebateStatus = 6

    LEFT JOIN `dbe_dbx_internships`.`datastore`.`SMRBIPdsRebateTableStaging` S3
        ON S.PdsRebateId =
                CASE
                    WHEN S3.PdsRebateId LIKE '%/C%' THEN
                        TRIM(SUBSTRING(S3.PdsRebateId, 1, INSTR(S3.PdsRebateId, '/C') - 1))
                    ELSE S3.PdsRebateId
                END
        AND S.DataAreaId = S3.DataAreaId
        AND S.SalesInvoiceId = S3.SalesInvoiceId
        AND S.ItemId = S3.ItemId
        AND S.CustInvoiceTransRefRecId = S3.CustInvoiceTransRefRecId
        AND S3.PdsRebateStatus = 9
WHERE 1=1                -- placeholder for future WHERE clauses
GROUP BY
    S.DataAreaId,
    S.PdsRebateId,
    S.SalesInvoiceId,
    S.ItemId,
    S.CustAccount,
    S.CurrencyCode,
    S.PdsStartingRebateAmt,
    S.CustInvoiceTransRefRecId,
    S1.PdsCorrectedRebateAmt
;
"""

# COMMAND ----------

# Execute the query – this creates a persistent Unity Catalog view
spark.sql(sql_create_view)

# COMMAND ----------

# Example query to verify the view
df = spark.sql("SELECT * FROM `dbe_dbx_internships`.`datastore`.`V_SalesRebate`")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 4483)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN -- Create or replace the persistent view in Unity Catalog CREATE OR REPLACE VIEW `_placeholder_`.`_placeholder_`.`V_SalesRebate` AS SELECT     UPPER(S.DataAreaId)                            AS CompanyCode,     S.PdsRebateId                                 AS SalesRebateCode,     S.SalesInvoiceId                              AS SalesInvoiceCode,     S.CustInvoiceTransRefRecId                    AS SalesInvoiceLineId,     -- ProductCode: if ItemId is NULL or empty → '_N/A'     IFNULL(NULLIF(UPPER(S.ItemId), ''), '_N/A')    AS ProductCode,     S.CustAccount                                  AS RebateCustomerCode,     S.CurrencyCode                                AS RebateCurrencyCode,     S.PdsStartingRebateAmt                        AS RebateAmountOriginal,      -- Total of corrected rebates that are COMPLETED (status 5)     SUM(COALESCE(S1.PdsCorrectedRebateAmt, 0))    AS RebateAmountCompleted,      -- Total of corrected rebates that are MARKED (status 6)     SUM(COALESCE(S2.PdsCorrectedRebateAmt, 0))    AS RebateAmountMarked,      -- Total of corrected rebates that are CANCELLED (status 9)     SUM(COALESCE(S3.PdsCorrectedRebateAmt, 0))    AS RebateAmountCancelled,      -- Net variance compared to the original amount     CASE         WHEN COALESCE(S1.PdsCorrectedRebateAmt, 0) = 0 THEN 0         ELSE SUM(COALESCE(S1.PdsCorrectedRebateAmt, 0) - S.PdsStartingRebateAmt)     END                                            AS RebateAmountVariance FROM     -- -----------------------------------------------------------------     -- 1. Deduplicate staging rows – Normalised PdsRebateId     -- -----------------------------------------------------------------     (         SELECT DISTINCT             CASE                 WHEN S.PdsRebateId LIKE '%/C%' THEN                     TRIM(SUBSTRING(S.PdsRebateId, 1, INSTR(S.PdsRebateId, '/C') - 1))                 ELSE S.PdsRebateId             END                                AS PdsRebateId,             S.CurrencyCode,             S.CustAccount,             S.DataAreaId,             S.ItemId,             S.SalesInvoiceId,             S.CustInvoiceTransRefRecId,             S.PdsStartingRebateAmt         FROM `_placeholder_`.`_placeholder_`.`SMRBIPdsRebateTableStaging` S     ) S     -- -----------------------------------------------------------------     -- 2. Left-join with status 5, 6 and 9 rows (the same normalisation     --    logic is applied in the ON clause)     -- -----------------------------------------------------------------     LEFT JOIN `_placeholder_`.`_placeholder_`.`SMRBIPdsRebateTableStaging` S1         ON S.PdsRebateId =                 CASE                     WHEN S1.PdsRebateId LIKE '%/C%' THEN                         TRIM(SUBSTRING(S1.PdsRebateId, 1, INSTR(S1.PdsRebateId, '/C') - 1))                     ELSE S1.PdsRebateId                 END         AND S.DataAreaId = S1.DataAreaId         AND S.SalesInvoiceId = S1.SalesInvoiceId         AND S.ItemId = S1.ItemId         AND S.CustInvoiceTransRefRecId = S1.CustInvoiceTransRefRecId         AND S1.PdsRebateStatus = 5      LEFT JOIN `_placeholder_`.`_placeholder_`.`SMRBIPdsRebateTableStaging` S2         ON S.PdsRebateId =                 CASE                     WHEN S2.PdsRebateId LIKE '%/C%' THEN                         TRIM(SUBSTRING(S2.PdsRebateId, 1, INSTR(S2.PdsRebateId, '/C') - 1))                     ELSE S2.PdsRebateId                 END         AND S.DataAreaId = S2.DataAreaId         AND S.SalesInvoiceId = S2.SalesInvoiceId         AND S.ItemId = S2.ItemId         AND S.CustInvoiceTransRefRecId = S2.CustInvoiceTransRefRecId         AND S2.PdsRebateStatus = 6      LEFT JOIN `_placeholder_`.`_placeholder_`.`SMRBIPdsRebateTableStaging` S3         ON S.PdsRebateId =                 CASE                     WHEN S3.PdsRebateId LIKE '%/C%' THEN                         TRIM(SUBSTRING(S3.PdsRebateId, 1, INSTR(S3.PdsRebateId, '/C') - 1))                     ELSE S3.PdsRebateId                 END         AND S.DataAreaId = S3.DataAreaId         AND S.SalesInvoiceId = S3.SalesInvoiceId         AND S.ItemId = S3.ItemId         AND S.CustInvoiceTransRefRecId = S3.CustInvoiceTransRefRecId         AND S3.PdsRebateStatus = 9 WHERE 1=1                -- placeholder for future WHERE clauses GROUP BY     S.DataAreaId,     S.PdsRebateId,     S.SalesInvoiceId,     S.ItemId,     S.CustAccount,     S.CurrencyCode,     S.PdsStartingRebateAmt,     S.CustInvoiceTransRefRecId,     S1.PdsCorrectedRebateAmt ;
# MAGIC -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
