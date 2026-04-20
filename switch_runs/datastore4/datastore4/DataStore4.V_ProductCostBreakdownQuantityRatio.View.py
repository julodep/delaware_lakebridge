# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore4.V_ProductCostBreakdownQuantityRatio.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore4/datastore4_volume/datastore4/DataStore4.V_ProductCostBreakdownQuantityRatio.View.sql`

# COMMAND ----------

# Create or replace the persistent view in Databricks
spark.sql(f"""
CREATE OR REPLACE VIEW `{catalog}`.`{schema}`.`V_ProductCostBreakdownQuantityRatio` AS
SELECT DISTINCT
    T1.`DataAreaId`,
    T1.`ItemNumber`                                            AS ItemNumber,

    /* ----------------------- CalculationNr -----------------------
       Build a concatenated string from `ConsistOfPrice` and
       `PriceCalcId` of T2…T6 by extracting the first non‑zero
       character after the third position of each column.  The
       extracted substrings are concatenated, then cast to
       DECIMAL(38,0). */
    CAST(
        COALESCE(
            -- Concatenate all parts
            COALESCE(
                regexp_extract(COALESCE(T1.`ConsistOfPrice`,''),
                               '^.{2}0*([1-9].*)$',1),
                ''
            )
          +
            COALESCE(
                regexp_extract(COALESCE(T1.`PriceCalcId`,''),
                               '^.{2}0*([1-9].*)$',1),
                ''
            )
          +
            COALESCE(
                regexp_extract(COALESCE(T2.`PriceCalcId`,''),
                               '^.{2}0*([1-9].*)$',1),
                ''
            )
          +
            COALESCE(
                regexp_extract(COALESCE(T3.`PriceCalcId`,''),
                               '^.{2}0*([1-9].*)$',1),
                ''
            )
          +
            COALESCE(
                regexp_extract(COALESCE(T4.`PriceCalcId`,''),
                               '^.{2}0*([1-9].*)$',1),
                ''
            )
          +
            COALESCE(
                regexp_extract(COALESCE(T5.`PriceCalcId`,''),
                               '^.{2}0*([1-9].*)$',1),
                ''
            )
          +
            COALESCE(
                regexp_extract(COALESCE(T6.`PriceCalcId`,''),
                               '^.{2}0*([1-9].*)$',1),
                ''
            ),
            ''
        ),
        DECIMAL(38,0)
    ) AS `CalculationNr`,

    /* --------------------------- Price columns ----------------- */
    T1.`ConsistOfPrice`                          AS `PriceCalcId`,
    T1.`PriceCalcId`                            AS `T1_PriceCalcId`,
    T2.`PriceCalcId`                            AS `T2_PriceCalcId`,
    T3.`PriceCalcId`                            AS `T3_PriceCalcId`,
    T4.`PriceCalcId`                            AS `T4_PriceCalcId`,
    T5.`PriceCalcId`                            AS `T5_PriceCalcId`,
    T6.`PriceCalcId`                            AS `T6_PriceCalcId`,

    /* --------------------------- QtyRatio columns -------------- */
    COALESCE(T1.`QtyRatio`, 1)                   AS `QtyRatioP0`,
    COALESCE(T2.`QtyRatio`, 1)                   AS `QtyRatioP1`,
    COALESCE(T3.`QtyRatio`, 1)                   AS `QtyRatioP2`,
    COALESCE(T4.`QtyRatio`, 1)                   AS `QtyRatioP3`,
    COALESCE(T5.`QtyRatio`, 1)                   AS `QtyRatioP4`,
    COALESCE(T6.`QtyRatio`, 1)                   AS `QtyRatioP5`,

    /* --------------------- TotalQtyRatio ------------------------ */
    CAST(
        COALESCE(CAST(T1.`QtyRatio` AS FLOAT), 1) *
        COALESCE(CAST(T2.`QtyRatio` AS FLOAT), 1) *
        COALESCE(CAST(T3.`QtyRatio` AS FLOAT), 1) *
        COALESCE(CAST(T4.`QtyRatio` AS FLOAT), 1) *
        COALESCE(CAST(T5.`QtyRatio` AS FLOAT), 1) *
        COALESCE(CAST(T6.`QtyRatio` AS FLOAT), 1)
    , DECIMAL(38,17))                                AS `TotalQtyRatio`

FROM `{catalog}`.`{schema}`.`ProductCostBreakdownQuantityRatioTMP` T1
LEFT JOIN `{catalog}`.`{schema}`.`ProductCostBreakdownQuantityRatioTMP` T2
    ON T1.`DataAreaId` = T2.`DataAreaId`
    AND T1.`ItemNumber` = T2.`ItemNumber`
    AND T1.`PriceCalcId` = T2.`ConsistOfPrice`
LEFT JOIN `{catalog}`.`{schema}`.`ProductCostBreakdownQuantityRatioTMP` T3
    ON T2.`DataAreaId` = T3.`DataAreaId`
    AND T2.`ItemNumber` = T3.`ItemNumber`
    AND T2.`PriceCalcId` = T3.`ConsistOfPrice`
LEFT JOIN `{catalog}`.`{schema}`.`ProductCostBreakdownQuantityRatioTMP` T4
    ON T3.`DataAreaId` = T4.`DataAreaId`
    AND T3.`ItemNumber` = T4.`ItemNumber`
    AND T3.`PriceCalcId` = T4.`ConsistOfPrice`
LEFT JOIN `{catalog}`.`{schema}`.`ProductCostBreakdownQuantityRatioTMP` T5
    ON T4.`DataAreaId` = T5.`DataAreaId`
    AND T4.`ItemNumber` = T5.`ItemNumber`
    AND T4.`PriceCalcId` = T5.`ConsistOfPrice`
LEFT JOIN `{catalog}`.`{schema}`.`ProductCostBreakdownQuantityRatioTMP` T6
    ON T5.`DataAreaId` = T6.`DataAreaId`
    AND T5.`ItemNumber` = T6.`ItemNumber`
    AND T5.`PriceCalcId` = T6.`ConsistOfPrice`

WHERE 1 = 1
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 4610)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE VIEW `_placeholder_`.`_placeholder_`.`V_ProductCostBreakdownQuantityRatio` AS SELECT DISTINCT     T1.`DataAreaId`,     T1.`ItemNumber`                                            AS ItemNumber,      /* ----------------------- CalculationNr -----------------------        Build a concatenated string from `ConsistOfPrice` and        `PriceCalcId` of T2…T6 by extracting the first non‑zero        character after the third position of each column.  The        extracted substrings are concatenated, then cast to        DECIMAL(38,0). */     CAST(         COALESCE(             -- Concatenate all parts             COALESCE(                 regexp_extract(COALESCE(T1.`ConsistOfPrice`,''),                                '^.20*([1-9].*)$',1),                 ''             )           +             COALESCE(                 regexp_extract(COALESCE(T1.`PriceCalcId`,''),                                '^.20*([1-9].*)$',1),                 ''             )           +             COALESCE(                 regexp_extract(COALESCE(T2.`PriceCalcId`,''),                                '^.20*([1-9].*)$',1),                 ''             )           +             COALESCE(                 regexp_extract(COALESCE(T3.`PriceCalcId`,''),                                '^.20*([1-9].*)$',1),                 ''             )           +             COALESCE(                 regexp_extract(COALESCE(T4.`PriceCalcId`,''),                                '^.20*([1-9].*)$',1),                 ''             )           +             COALESCE(                 regexp_extract(COALESCE(T5.`PriceCalcId`,''),                                '^.20*([1-9].*)$',1),                 ''             )           +             COALESCE(                 regexp_extract(COALESCE(T6.`PriceCalcId`,''),                                '^.20*([1-9].*)$',1),                 ''             ),             ''         ),         DECIMAL(38,0)     ) AS `CalculationNr`,      /* --------------------------- Price columns ----------------- */     T1.`ConsistOfPrice`                          AS `PriceCalcId`,     T1.`PriceCalcId`                            AS `T1_PriceCalcId`,     T2.`PriceCalcId`                            AS `T2_PriceCalcId`,     T3.`PriceCalcId`                            AS `T3_PriceCalcId`,     T4.`PriceCalcId`                            AS `T4_PriceCalcId`,     T5.`PriceCalcId`                            AS `T5_PriceCalcId`,     T6.`PriceCalcId`                            AS `T6_PriceCalcId`,      /* --------------------------- QtyRatio columns -------------- */     COALESCE(T1.`QtyRatio`, 1)                   AS `QtyRatioP0`,     COALESCE(T2.`QtyRatio`, 1)                   AS `QtyRatioP1`,     COALESCE(T3.`QtyRatio`, 1)                   AS `QtyRatioP2`,     COALESCE(T4.`QtyRatio`, 1)                   AS `QtyRatioP3`,     COALESCE(T5.`QtyRatio`, 1)                   AS `QtyRatioP4`,     COALESCE(T6.`QtyRatio`, 1)                   AS `QtyRatioP5`,      /* --------------------- TotalQtyRatio ------------------------ */     CAST(         COALESCE(CAST(T1.`QtyRatio` AS FLOAT), 1) *         COALESCE(CAST(T2.`QtyRatio` AS FLOAT), 1) *         COALESCE(CAST(T3.`QtyRatio` AS FLOAT), 1) *         COALESCE(CAST(T4.`QtyRatio` AS FLOAT), 1) *         COALESCE(CAST(T5.`QtyRatio` AS FLOAT), 1) *         COALESCE(CAST(T6.`QtyRatio` AS FLOAT), 1)     , DECIMAL(38,17))                                AS `TotalQtyRatio`  FROM `_placeholder_`.`_placeholder_`.`ProductCostBreakdownQuantityRatioTMP` T1 LEFT JOIN `_placeholder_`.`_placeholder_`.`ProductCostBreakdownQuantityRatioTMP` T2     ON T1.`DataAreaId` = T2.`DataAreaId`     AND T1.`ItemNumber` = T2.`ItemNumber`     AND T1.`PriceCalcId` = T2.`ConsistOfPrice` LEFT JOIN `_placeholder_`.`_placeholder_`.`ProductCostBreakdownQuantityRatioTMP` T3     ON T2.`DataAreaId` = T3.`DataAreaId`     AND T2.`ItemNumber` = T3.`ItemNumber`     AND T2.`PriceCalcId` = T3.`ConsistOfPrice` LEFT JOIN `_placeholder_`.`_placeholder_`.`ProductCostBreakdownQuantityRatioTMP` T4     ON T3.`DataAreaId` = T4.`DataAreaId`     AND T3.`ItemNumber` = T4.`ItemNumber`     AND T3.`PriceCalcId` = T4.`ConsistOfPrice` LEFT JOIN `_placeholder_`.`_placeholder_`.`ProductCostBreakdownQuantityRatioTMP` T5     ON T4.`DataAreaId` = T5.`DataAreaId`     AND T4.`ItemNumber` = T5.`ItemNumber`     AND T4.`PriceCalcId` = T5.`ConsistOfPrice` LEFT JOIN `_placeholder_`.`_placeholder_`.`ProductCostBreakdownQuantityRatioTMP` T6     ON T5.`DataAreaId` = T6.`DataAreaId`     AND T5.`ItemNumber` = T6.`ItemNumber`     AND T5.`PriceCalcId` = T6.`ConsistOfPrice`  WHERE 1 = 1
# MAGIC --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
