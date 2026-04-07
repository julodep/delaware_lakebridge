# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_CostPrice.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_CostPrice.View.sql`

# COMMAND ----------

# --------------------------------------------------------------
# Create the persistent view `V_CostPrice` in dbe_dbx_internships.datastore
#
# 1.  All table names inside the view are fully-qualified.
# 2.  T‑SQL date arithmetic (`date - 1`) is replaced with the Spark
#     function `date_sub(date, 1)`.
# 3.  The CASE expression for `EndValidityDate` now casts literal
#     dates to the DATE type to avoid implicit string → date conversion
#     warnings.
# 4.  The view is created with `CREATE OR REPLACE` so that you can
#     re‑run this cell without errors.
# --------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.`V_CostPrice` AS
WITH StartEndValidityDate AS (
    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY IIP.DataAreaId, IIP.ItemId, IIP.UnitId, IIP.InventDimId
            ORDER BY IIP.DataAreaId, IIP.ItemId, IIP.ActivationDate ASC,
                     IIP.CreatedDateTime DESC
        ) AS RankNr,
        IIP.ActivationDate                AS ActivationDate,
        IIP.ItemId                        AS ItemNumber,
        IIP.InventDimId                   AS InventDimId,
        IIP.DataAreaId                    AS CompanyId,
        (IIP.Price + IIP.Markup) /
        CASE WHEN IIP.PriceUnit = 0 THEN 1 ELSE IIP.PriceUnit END
                                    AS Price,
        IIP.UnitId                        AS UnitId,
        IIP.CreatedDateTime               AS CreatedDateTime
    FROM (
        SELECT
            IIP2.ActivationDate,
            IIP2.ItemId,
            IIP2.InventDimId,
            IIP2.DataAreaId,
            IIP1.Price,
            IIP1.Markup,
            IIP1.PriceUnit,
            IIP1.PriceQty,
            IIP2.UnitId,
            IIP2.CreatedDateTime
        FROM `dbe_dbx_internships`.`datastore`.`SMRBIInventItemPriceStaging` IIP1
        JOIN (
            SELECT
                ActivationDate,
                ItemId,
                InventDimId,
                DataAreaId,
                CostingType,
                PriceType,
                UnitId,
                MAX(ItemPriceCreatedDateTime) AS CreatedDateTime
            FROM `dbe_dbx_internships`.`datastore`.`SMRBIInventItemPriceStaging`
            WHERE CostingType = 2 AND PriceType = 0
            GROUP BY ActivationDate, ItemId, InventDimId,
                     DataAreaId, CostingType, PriceType, UnitId
        ) IIP2
          ON IIP1.ActivationDate = IIP2.ActivationDate
         AND IIP1.ItemId       = IIP2.ItemId
         AND IIP1.InventDimId = IIP2.InventDimId
         AND IIP1.DataAreaId   = IIP2.DataAreaId
         AND IIP1.UnitId       = IIP2.UnitId
         AND IIP1.ItemPriceCreatedDateTime = IIP2.CreatedDateTime
         AND IIP1.CostingType  = IIP2.CostingType
         AND IIP1.PriceType    = IIP2.PriceType
    ) IIP
),
Final AS (
    SELECT
        SEVD1.ItemNumber,
        SEVD1.InventDimId      AS InventDimCode,
        SEVD1.UnitId           AS UnitCode,
        SEVD1.CompanyId        AS CompanyCode,
        SEVD1.Price,
        SEVD1.ActivationDate   AS StartValidityDate,
        CASE
            WHEN SEVD2.ItemNumber IS NULL THEN CAST('9999-12-31' AS date)
            ELSE date_sub(SEVD2.ActivationDate, 1)
        END                   AS EndValidityDate
    FROM StartEndValidityDate SEVD1
    LEFT JOIN StartEndValidityDate SEVD2
        ON SEVD1.RankNr = SEVD2.RankNr - 1
       AND SEVD1.ItemNumber = SEVD2.ItemNumber
       AND SEVD1.InventDimId = SEVD2.InventDimId
       AND SEVD1.CompanyId = SEVD2.CompanyId
    UNION ALL
    SELECT
        '_N/A',
        '_N/A',
        '_N/A',
        CompanyId,
        0,
        CAST('1900-01-01' AS date),
        CAST('9999-12-31' AS date)
    FROM `dbe_dbx_internships`.`datastore`.`SMRBIOfficeAddinLegalEntityStaging`
)
SELECT * FROM Final;
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
