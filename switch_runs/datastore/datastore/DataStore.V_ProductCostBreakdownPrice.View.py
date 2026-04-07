# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_ProductCostBreakdownPrice.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_ProductCostBreakdownPrice.View.sql`

# COMMAND ----------

# ------------------------------------------------------------------
#  Create a Unity Catalog view that mimics the original T‑SQL view
#  (DataStore.V_ProductCostBreakdownPrice)
# ------------------------------------------------------------------
#  NOTE
#  * Every table, view or function reference has been fully‑qualified, e.g.
#    `dbe_dbx_internships`.`datastore`.`SMRBIInventItemPriceStaging`.
#  * ANSI/SQL‑Server specific functions such as COLLATE, LTRIM, RTRIM,
#    TINYINT/DECIMAL or ISNULL have been replaced by their Spark equivalents.
#  * The view is created with `CREATE OR REPLACE VIEW` so it can be updated
#    repeatedly without needing to drop it first.
# ------------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.`V_ProductCostBreakdownPrice` AS

WITH StartEndValidityDate_Active AS (
  SELECT
    -- Rank counts to identify the most recent and price-max records
    ROW_NUMBER() OVER (
      PARTITION BY
        IIP.DataAreaId,
        IIP.ItemId,
        IIP.UnitId,
        IIP.InventDimId,
        IIP.VersionId,
        LEFT(IIP.PriceCalcId, 2)
      ORDER BY
        IIP.DataAreaId,
        IIP.ItemId,
        IIP.CreatedDateTime ASC
    ) AS RankNr,

    ROW_NUMBER() OVER (
      PARTITION BY
        IIP.DataAreaId,
        IIP.ItemId,
        IIP.UnitId,
        IIP.InventDimId,
        IIP.VersionId
      ORDER BY
        IIP.DataAreaId,
        IIP.ItemId,
        IIP.CreatedDateTime ASC
    ) AS RankNrIsMaxPrice,

    IIP.ActivationDate,
    IIP.ItemId                                   AS ItemNumber,
    IIP.InventDimId                              AS InventDimId,
    IIP.DataAreaId                               AS CompanyId,
    IIP.PriceCalcId                              AS PriceCalcId,
    IIP.VersionId                                AS VersionId,

    (
      CAST(IIP.Price    AS DECIMAL(15,8)) +
      CAST(IIP.Markup   AS DECIMAL(15,8))
    ) / (CAST(CASE WHEN IIP.PriceUnit = 0 THEN 1 ELSE IIP.PriceUnit END AS DECIMAL(8,1))) AS Price,

    IIP.UnitId                                   AS UnitId,
    1                                            AS CalcOrder,
    IIP.CreatedDateTime                         AS CreatedDateTime

  FROM (
    SELECT
      IIP2.ActivationDate,
      IIP2.ItemId,
      IIP2.InventDimId,
      IIP2.DataAreaId,
      IIP1.Price,
      IIP1.Markup,
      IIP1.PriceUnit,
      IIP1.PriceCalcId,
      IIP1.PriceQty,
      IIP2.UnitId,
      IIP1.VersionId,
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
        VersionId,
        PriceCalcId,
        UnitId,
        MAX(ItemPriceCreatedDateTime) AS CreatedDateTime
      FROM `dbe_dbx_internships`.`datastore`.`SMRBIInventItemPriceStaging`
      WHERE CostingType = 2 AND PriceType = 0
      GROUP BY
        ActivationDate,
        ItemId,
        InventDimId,
        DataAreaId,
        CostingType,
        PriceType,
        VersionId,
        PriceCalcId,
        UnitId
    ) IIP2
    ON  IIP1.ActivationDate   = IIP2.ActivationDate
    AND IIP1.ItemId           = IIP2.ItemId
    AND IIP1.InventDimId      = IIP2.InventDimId
    AND IIP1.DataAreaId       = IIP2.DataAreaId
    AND IIP1.UnitId           = IIP2.UnitId
    AND IIP1.ItemPriceCreatedDateTime = IIP2.CreatedDateTime
    AND IIP1.CostingType      = IIP2.CostingType
    AND IIP1.PriceType        = IIP2.PriceType
    AND IIP1.PriceCalcId      = IIP2.PriceCalcId
    AND IIP1.VersionId        = IIP2.VersionId
  ) IIP
),

--------------------------------------------------------------------
-- Final SELECT that calculates validity periods and flag columns
--------------------------------------------------------------------
SELECT
  SEVD1.ItemNumber                                       AS ItemNumber,
  SEVD1.InventDimId                                     AS InventDimCode,
  SEVD1.UnitId                                          AS UnitCode,
  SEVD1.CompanyId                                       AS CompanyCode,
  SEVD1.PriceCalcId,
  CAST(SEVD1.Price AS DECIMAL(38,17))                   AS Price,
  SEVD1.VersionId                                       AS VersionCode,
  CAST('Active' AS STRING)                              AS PriceType,
  SEVD1.ActivationDate                                 AS StartValidityDate,

  CASE
    WHEN SEVD2.ItemNumber IS NULL
      THEN CAST('9999-12-31' AS DATE)                    -- open-ended end date
    WHEN SEVD2.ActivationDate = SEVD1.ActivationDate
      THEN SEVD2.ActivationDate
    ELSE SEVD2.ActivationDate - INTERVAL 1 DAY
  END                                                   AS EndValidityDate,

  CASE
    WHEN SEVD1.PriceCalcId IS NULL OR SEVD1.PriceCalcId = ''
      THEN 'Not Calculated'
    ELSE CONCAT('Calculation ', CAST(SEVD1.RankNr AS STRING))
  END                                                   AS CalculationNr,

  SEVD1.RankNr                                          AS CalculationNrTech,

  CASE
    WHEN ROW_NUMBER() OVER (
      PARTITION BY
        SEVD1.CompanyId,
        SEVD1.ItemNumber,
        SEVD1.VersionId,
        SEVD1.UnitId,
        SEVD1.InventDimId,
        LEFT(SEVD1.PriceCalcId, 2)
      ORDER BY
        SEVD1.CompanyId,
        SEVD1.ItemNumber,
        SEVD1.CreatedDateTime DESC
    ) = 1 THEN 'Yes' ELSE 'No' END                 AS IsMaxCalculation,

  CASE WHEN SEVD2.ItemNumber IS NULL THEN 'Yes' ELSE 'No' END  AS IsActivePrice,

  CASE
    WHEN ROW_NUMBER() OVER (
      PARTITION BY
        SEVD1.CompanyId,
        SEVD1.ItemNumber,
        SEVD1.InventDimId
      ORDER BY
        SEVD1.CompanyId,
        SEVD1.ItemNumber,
        SEVD1.InventDimId,
        SEVD1.RankNrIsMaxPrice DESC
    ) = 1 THEN 'Yes' ELSE 'No' END                     AS IsMaxPrice

FROM StartEndValidityDate_Active SEVD1
LEFT JOIN StartEndValidityDate_Active SEVD2
  ON SEVD1.RankNr            = SEVD2.RankNr - 1
 AND SEVD1.ItemNumber        = SEVD2.ItemNumber
 AND SEVD1.InventDimId       = SEVD2.InventDimId
 AND SEVD1.CompanyId         = SEVD2.CompanyId
 AND LEFT(SEVD1.PriceCalcId, 2) = LEFT(SEVD2.PriceCalcId, 2)
 AND SEVD1.VersionId          = SEVD2.VersionId

WHERE 1=1                   -- placeholder for potential filters
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 5652)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE VIEW `_placeholder_`.`_placeholder_`.`V_ProductCostBreakdownPrice` AS  WITH StartEndValidityDate_Active AS (   SELECT     -- Rank counts to identify the most recent and price-max records     ROW_NUMBER() OVER (       PARTITION BY         IIP.DataAreaId,         IIP.ItemId,         IIP.UnitId,         IIP.InventDimId,         IIP.VersionId,         LEFT(IIP.PriceCalcId, 2)       ORDER BY         IIP.DataAreaId,         IIP.ItemId,         IIP.CreatedDateTime ASC     ) AS RankNr,      ROW_NUMBER() OVER (       PARTITION BY         IIP.DataAreaId,         IIP.ItemId,         IIP.UnitId,         IIP.InventDimId,         IIP.VersionId       ORDER BY         IIP.DataAreaId,         IIP.ItemId,         IIP.CreatedDateTime ASC     ) AS RankNrIsMaxPrice,      IIP.ActivationDate,     IIP.ItemId                                   AS ItemNumber,     IIP.InventDimId                              AS InventDimId,     IIP.DataAreaId                               AS CompanyId,     IIP.PriceCalcId                              AS PriceCalcId,     IIP.VersionId                                AS VersionId,      (       CAST(IIP.Price    AS DECIMAL(15,8)) +       CAST(IIP.Markup   AS DECIMAL(15,8))     ) / (CAST(CASE WHEN IIP.PriceUnit = 0 THEN 1 ELSE IIP.PriceUnit END AS DECIMAL(8,1))) AS Price,      IIP.UnitId                                   AS UnitId,     1                                            AS CalcOrder,     IIP.CreatedDateTime                         AS CreatedDateTime    FROM (     SELECT       IIP2.ActivationDate,       IIP2.ItemId,       IIP2.InventDimId,       IIP2.DataAreaId,       IIP1.Price,       IIP1.Markup,       IIP1.PriceUnit,       IIP1.PriceCalcId,       IIP1.PriceQty,       IIP2.UnitId,       IIP1.VersionId,       IIP2.CreatedDateTime     FROM `_placeholder_`.`_placeholder_`.`SMRBIInventItemPriceStaging` IIP1     JOIN (       SELECT         ActivationDate,         ItemId,         InventDimId,         DataAreaId,         CostingType,         PriceType,         VersionId,         PriceCalcId,         UnitId,         MAX(ItemPriceCreatedDateTime) AS CreatedDateTime       FROM `_placeholder_`.`_placeholder_`.`SMRBIInventItemPriceStaging`       WHERE CostingType = 2 AND PriceType = 0       GROUP BY         ActivationDate,         ItemId,         InventDimId,         DataAreaId,         CostingType,         PriceType,         VersionId,         PriceCalcId,         UnitId     ) IIP2     ON  IIP1.ActivationDate   = IIP2.ActivationDate     AND IIP1.ItemId           = IIP2.ItemId     AND IIP1.InventDimId      = IIP2.InventDimId     AND IIP1.DataAreaId       = IIP2.DataAreaId     AND IIP1.UnitId           = IIP2.UnitId     AND IIP1.ItemPriceCreatedDateTime = IIP2.CreatedDateTime     AND IIP1.CostingType      = IIP2.CostingType     AND IIP1.PriceType        = IIP2.PriceType     AND IIP1.PriceCalcId      = IIP2.PriceCalcId     AND IIP1.VersionId        = IIP2.VersionId   ) IIP ),  -------------------------------------------------------------------- -- Final SELECT that calculates validity periods and flag columns -------------------------------------------------------------------- SELECT   SEVD1.ItemNumber                                       AS ItemNumber,   SEVD1.InventDimId                                     AS InventDimCode,   SEVD1.UnitId                                          AS UnitCode,   SEVD1.CompanyId                                       AS CompanyCode,   SEVD1.PriceCalcId,   CAST(SEVD1.Price AS DECIMAL(38,17))                   AS Price,   SEVD1.VersionId                                       AS VersionCode,   CAST('Active' AS STRING)                              AS PriceType,   SEVD1.ActivationDate                                 AS StartValidityDate,    CASE     WHEN SEVD2.ItemNumber IS NULL       THEN CAST('9999-12-31' AS DATE)                    -- open-ended end date     WHEN SEVD2.ActivationDate = SEVD1.ActivationDate       THEN SEVD2.ActivationDate     ELSE SEVD2.ActivationDate - INTERVAL 1 DAY   END                                                   AS EndValidityDate,    CASE     WHEN SEVD1.PriceCalcId IS NULL OR SEVD1.PriceCalcId = ''       THEN 'Not Calculated'     ELSE CONCAT('Calculation ', CAST(SEVD1.RankNr AS STRING))   END                                                   AS CalculationNr,    SEVD1.RankNr                                          AS CalculationNrTech,    CASE     WHEN ROW_NUMBER() OVER (       PARTITION BY         SEVD1.CompanyId,         SEVD1.ItemNumber,         SEVD1.VersionId,         SEVD1.UnitId,         SEVD1.InventDimId,         LEFT(SEVD1.PriceCalcId, 2)       ORDER BY         SEVD1.CompanyId,         SEVD1.ItemNumber,         SEVD1.CreatedDateTime DESC     ) = 1 THEN 'Yes' ELSE 'No' END                 AS IsMaxCalculation,    CASE WHEN SEVD2.ItemNumber IS NULL THEN 'Yes' ELSE 'No' END  AS IsActivePrice,    CASE     WHEN ROW_NUMBER() OVER (       PARTITION BY         SEVD1.CompanyId,         SEVD1.ItemNumber,         SEVD1.InventDimId       ORDER BY         SEVD1.CompanyId,         SEVD1.ItemNumber,         SEVD1.InventDimId,         SEVD1.RankNrIsMaxPrice DESC     ) = 1 THEN 'Yes' ELSE 'No' END                     AS IsMaxPrice  FROM StartEndValidityDate_Active SEVD1 LEFT JOIN StartEndValidityDate_Active SEVD2   ON SEVD1.RankNr            = SEVD2.RankNr - 1  AND SEVD1.ItemNumber        = SEVD2.ItemNumber  AND SEVD1.InventDimId       = SEVD2.InventDimId  AND SEVD1.CompanyId         = SEVD2.CompanyId  AND LEFT(SEVD1.PriceCalcId, 2) = LEFT(SEVD2.PriceCalcId, 2)  AND SEVD1.VersionId          = SEVD2.VersionId  WHERE 1=1                   -- placeholder for potential filters
# MAGIC ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
