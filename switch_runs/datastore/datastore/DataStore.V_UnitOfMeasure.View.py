# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_UnitOfMeasure.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_UnitOfMeasure.View.sql`

# COMMAND ----------

# ---------------------------------------------------------------------
#  Databricks notebook: Create the persistent view V_UnitOfMeasure
#
#  Reference objects using the fully‑qualified format:
#      dbe_dbx_internships.datastore.{object_name}
#  Populate the `catalog` and `schema` variables with your actual values
#  before running the notebook.
# ---------------------------------------------------------------------

# ----- Configuration ----------------------------------------------------
catalog = "YOUR_CATALOG"   # replace with your catalog name
schema  = "YOUR_SCHEMA"    # replace with your schema name

# COMMAND ----------

# ----- Create the view -----------------------------------------------
# All T‑SQL constructs that are not directly supported in Spark
# (e.g. SYSTEM_USER, GO, bracketed identifiers) have been replaced
# with Spark equivalents or with fully‑qualified identifiers.
# Functions such as ISNULL, NULLIF, CASE, CAST, and window
# functions retain their original syntax (Spark supports them).
#
# Note:  The view is created as a persistent object in Unity Catalog
# by using CREATE OR REPLACE VIEW.  All table references are fully
# qualified and no temporary objects are used.

spark.sql(f"""
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.`V_UnitOfMeasure` AS
WITH RequiredUnitConversions AS (
    SELECT D.*,
           CONV.Factor / CONV2.Factor AS Factor
    FROM (
        SELECT C.*
        FROM (
            -- Build the normalised source mapping
            SELECT DISTINCT
                C.CompanyId,
                A.ItemNumber,
                A.FromUOM,
                B.ToUOM
            FROM (
                SELECT DISTINCT
                    DataAreaId AS CompanyId,
                    ItemNumber,
                    UPPER(SalesUnitSymbol) AS FromUOM
                FROM `dbe_dbx_internships`.`datastore`.`SMRBISalesOrderLineStaging`
                UNION ALL
                SELECT DISTINCT
                    DataAreaId AS CompanyId,
                    ItemId,
                    UPPER(SalesUnit) AS FromUOM
                FROM `dbe_dbx_internships`.`datastore`.`SMRBICustInvoiceTransStaging`
                UNION ALL
                SELECT DISTINCT
                    DataAreaId AS CompanyId,
                    ItemId,
                    UPPER(SalesUnit) AS FromUOM
                FROM `dbe_dbx_internships`.`datastore`.`SMRBICustPackingSlipTransStaging`
            ) A
            CROSS JOIN (
                SELECT 'PCS'   AS ToUOM UNION ALL
                SELECT 'BOX'   AS ToUOM UNION ALL
                SELECT 'PAL'   AS ToUOM
            ) B
            CROSS JOIN `dbe_dbx_internships`.`datastore`.`SMRBIOfficeAddinLegalEntityStaging` C
            WHERE 1=1
              AND NULLIF(ItemNumber, '') IS NOT NULL
              AND FromUOM <> ToUOM

            UNION ALL

            SELECT DISTINCT
                C.CompanyId,
                A.ItemNumber,
                B.FromUOM,
                A.ToUOM
            FROM (
                SELECT DISTINCT
                    DataAreaId AS CompanyId,
                    ItemNumber,
                    UPPER(SalesUnitSymbol) AS ToUOM
                FROM `dbe_dbx_internships`.`datastore`.`SMRBISalesOrderLineStaging`
                UNION ALL
                SELECT DISTINCT
                    DataAreaId AS CompanyId,
                    ItemId,
                    UPPER(SalesUnit) AS ToUOM
                FROM `dbe_dbx_internships`.`datastore`.`SMRBICustInvoiceTransStaging`
                UNION ALL
                SELECT DISTINCT
                    DataAreaId AS CompanyId,
                    ItemId,
                    UPPER(SalesUnit) AS ToUOM
                FROM `dbe_dbx_internships`.`datastore`.`SMRBICustPackingSlipTransStaging`
            ) A
            CROSS JOIN (
                SELECT 'PCS'   AS FromUOM UNION ALL
                SELECT 'BOX'   AS FromUOM UNION ALL
                SELECT 'PAL'   AS FromUOM
            ) B
            CROSS JOIN `dbe_dbx_internships`.`datastore`.`SMRBIOfficeAddinLegalEntityStaging` C
            WHERE 1=1
              AND NULLIF(ItemNumber, '') IS NOT NULL
              AND FromUOM <> ToUOM
        ) C
        LEFT JOIN `dbe_dbx_internships`.`datastore`.`SMRBIEcoResProductSpecificUnitOfMeasureConversionStaging` CONV
            ON CONV.ProductNumber = C.ItemNumber
           AND CONV.FromUnitSymbol = C.FromUOM
           AND CONV.ToUnitSymbol   = C.ToUOM
        LEFT JOIN `dbe_dbx_internships`.`datastore`.`SMRBIEcoResProductSpecificUnitOfMeasureConversionStaging` CONV2
            ON CONV2.ProductNumber = C.ItemNumber
           AND CONV2.FromUnitSymbol = C.ToUOM
           AND CONV2.ToUnitSymbol   = C.FromUOM
        WHERE 1=1
          AND CONV.Factor IS NULL
    ) D
    LEFT JOIN `dbe_dbx_internships`.`datastore`.`SMRBIEcoResReleasedProductStaging` ERRPS
        ON ERRPS.DataAreaId = D.CompanyId
       AND ERRPS.ItemNumber = D.ItemNumber
    LEFT JOIN `dbe_dbx_internships`.`datastore`.`SMRBIEcoResProductSpecificUnitOfMeasureConversionStaging` CONV3
        ON CONV3.FromUnitSymbol = D.FromUOM
       AND CONV3.ToUnitSymbol   = ERRPS.SalesUnitSymbol
       AND CONV3.ProductNumber = D.ItemNumber
    LEFT JOIN `dbe_dbx_internships`.`datastore`.`SMRBIEcoResProductSpecificUnitOfMeasureConversionStaging` CONV4
        ON CONV4.FromUnitSymbol = D.ToUOM
       AND CONV4.ToUnitSymbol   = ERRPS.SalesUnitSymbol
       AND CONV4.ProductNumber = D.ItemNumber
    LEFT JOIN (
        SELECT ProductNumber,
               ToUnitSymbol
        FROM (
            SELECT
                ROW_NUMBER() OVER (PARTITION BY ProductNumber
                                   ORDER BY Nbr DESC,
                                            CASE WHEN ToUnitSymbol = 'SU' THEN 1 ELSE 99 END) AS RankNr,
                ProductNumber,
                ToUnitSymbol
            FROM (
                SELECT
                    ProductNumber,
                    ToUnitSymbol,
                    COUNT(*) AS Nbr
                FROM `dbe_dbx_internships`.`datastore`.`SMRBIEcoResProductSpecificUnitOfMeasureConversionStaging`
                GROUP BY ProductNumber, ToUnitSymbol
            ) A
        ) A
        GROUP BY RankNr, ProductNumber, ToUnitSymbol
        HAVING RankNr = 1
    ) TMP
        ON ERRPS.ItemNumber = TMP.ProductNumber
    LEFT JOIN `dbe_dbx_internships`.`datastore`.`SMRBIEcoResProductSpecificUnitOfMeasureConversionStaging` CONV5
        ON CONV5.FromUnitSymbol = D.FromUOM
       AND CONV5.ToUnitSymbol   = TMP.ToUnitSymbol
       AND CONV5.ProductNumber = D.ItemNumber
    LEFT JOIN `dbe_dbx_internships`.`datastore`.`SMRBIEcoResProductSpecificUnitOfMeasureConversionStaging` CONV6
        ON CONV6.FromUnitSymbol = D.ToUOM
       AND CONV6.ToUnitSymbol   = TMP.ToUnitSymbol
       AND CONV6.ProductNumber = D.ItemNumber
    WHERE 1=1
      AND CONV.Factor / CONV2.Factor IS NOT NULL
),

Result AS (
    SELECT DISTINCT
        UOMC.Denominator   AS Denominator,
        CAST('Normal' AS STRING) AS DataFlow,
        CAST(UOMC.Factor AS DECIMAL(32,17)) AS Factor,
        UOMC.InnerOffset  AS InnerOffset,
        UOMC.OuterOffset  AS OuterOffset,
        UOMC.ProductNumber AS Product,
        COALESCE(ERRPS.ItemNumber, '_N/A') AS ItemNumber,
        UOMC.Rounding     AS Rounding,
        UPPER(UOMC.FromUnitSymbol) AS FromUOM,
        UPPER(UOMC.ToUnitSymbol)   AS ToUOM,
        COALESCE(ERRPS.DataAreaId, '_N/A') AS CompanyCode
    FROM `dbe_dbx_internships`.`datastore`.`SMRBIEcoResProductSpecificUnitOfMeasureConversionStaging` UOMC
    LEFT JOIN `dbe_dbx_internships`.`datastore`.`SMRBIEcoResReleasedProductStaging` ERRPS
          ON ERRPS.ProductNumber = UOMC.ProductNumber

    UNION ALL

    SELECT
        UOMC.Denominator   AS Denominator,
        CAST('Normal|Non-Item' AS STRING) AS DataFlow,
        CAST(UOMC.Factor AS DECIMAL(32,17)) AS Factor,
        UOMC.InnerOffset  AS InnerOffset,
        UOMC.OuterOffset  AS OuterOffset,
        'NON-ITEM DRIVEN' AS Product,
        'NON-ITEM DRIVEN' AS ItemNumber,
        UOMC.Rounding     AS Rounding,
        UPPER(UOMC.FromUnitSymbol) AS FromUOM,
        UPPER(UOMC.ToUnitSymbol)   AS ToUOM,
        COALESCE(OALES.CompanyId, '_N/A') AS CompanyCode
    FROM `dbe_dbx_internships`.`datastore`.`SMRBIUnitOfMeasureConversionStaging` UOMC
    CROSS JOIN `dbe_dbx_internships`.`datastore`.`SMRBIOfficeAddinLegalEntityStaging` OALES

    UNION ALL

    SELECT
        UOMC.Denominator   AS Denominator,
        CAST('Normal (Rev)' AS STRING) AS DataFlow,
        ISNULL(1 / NULLIF(UOMC.Factor,0), 0)       AS Factor,
        ISNULL(1 / NULLIF(UOMC.InnerOffset,0), 0) AS InnerOffset,
        ISNULL(1 / NULLIF(UOMC.OuterOffset,0), 0) AS OuterOffset,
        UOMC.ProductNumber AS Product,
        COALESCE(ERRPS.ItemNumber, '_N/A') AS ItemNumber,
        UOMC.Rounding     AS Rounding,
        UPPER(UOMC.ToUnitSymbol) AS FromUOM,
        UPPER(UOMC.FromUnitSymbol) AS ToUOM,
        COALESCE(ERRPS.DataAreaId, '_N/A') AS CompanyCode
    FROM `dbe_dbx_internships`.`datastore`.`SMRBIEcoResProductSpecificUnitOfMeasureConversionStaging` UOMC
    LEFT JOIN `dbe_dbx_internships`.`datastore`.`SMRBIEcoResReleasedProductStaging` ERRPS
          ON ERRPS.ProductNumber = UOMC.ProductNumber

    UNION ALL

    SELECT
        UOMC.Denominator   AS Denominator,
        CAST('Normal|Non-Item' AS STRING) AS DataFlow,
        ISNULL(1 / NULLIF(UOMC.Factor,0), 0)       AS Factor,
        UOMC.InnerOffset   AS InnerOffset,
        UOMC.OuterOffset   AS OuterOffset,
        'NON-ITEM DRIVEN'   AS Product,
        'NON-ITEM DRIVEN'   AS ItemNumber,
        UOMC.Rounding      AS Rounding,
        UPPER(UOMC.ToUnitSymbol) AS FromUOM,
        UPPER(UOMC.FromUnitSymbol) AS ToUOM,
        COALESCE(OALES.CompanyId, '_N/A') AS CompanyCode
    FROM `dbe_dbx_internships`.`datastore`.`SMRBIUnitOfMeasureConversionStaging` UOMC
    CROSS JOIN `dbe_dbx_internships`.`datastore`.`SMRBIOfficeAddinLegalEntityStaging` OALES

    UNION ALL

    SELECT
        1 AS Denominator,
        CAST('Manual' AS STRING) AS DataFlow,
        CAST(RUC.Factor AS DECIMAL(32,17)) AS Factor,
        0 AS InnerOffset,
        0 AS OuterOffset,
        RUC.ItemNumber AS Product,
        RUC.ItemNumber AS ItemNumber,
        1 AS Rounding,
        RUC.FromUOM AS FromUOM,
        RUC.ToUOM AS ToUOM,
        RUC.CompanyId AS CompanyCode
    FROM `dbe_dbx_internships`.`datastore`.`RequiredUnitConversions` RUC

    UNION ALL

    SELECT
        -1 AS Denominator,
        CAST('Dummy' AS STRING) AS DataFlow,
        -1 AS Factor,
        -1 AS InnerOffset,
        -1 AS OuterOffset,
        '_N/A' AS Product,
        '_N/A' AS ItemNumber,
        -1 AS Rounding,
        '_N/A' AS FromUOM,
        '_N/A' AS ToUOM,
        UPPER(SM.CompanyId) AS CompanyCode   -- Dummy row for completeness
    FROM `dbe_dbx_internships`.`datastore`.`SMRBIOfficeAddinLegalEntityStaging` SM
)

SELECT *
FROM Result;
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 9612)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE VIEW `YOUR_CATALOG`.`YOUR_SCHEMA`.`V_UnitOfMeasure` AS WITH RequiredUnitConversions AS (     SELECT D.*,            CONV.Factor / CONV2.Factor AS Factor     FROM (         SELECT C.*         FROM (             -- Build the normalised source mapping             SELECT DISTINCT                 C.CompanyId,                 A.ItemNumber,                 A.FromUOM,                 B.ToUOM             FROM (                 SELECT DISTINCT                     DataAreaId AS CompanyId,                     ItemNumber,                     UPPER(SalesUnitSymbol) AS FromUOM                 FROM `YOUR_CATALOG`.`YOUR_SCHEMA`.`SMRBISalesOrderLineStaging`                 UNION ALL                 SELECT DISTINCT                     DataAreaId AS CompanyId,                     ItemId,                     UPPER(SalesUnit) AS FromUOM                 FROM `YOUR_CATALOG`.`YOUR_SCHEMA`.`SMRBICustInvoiceTransStaging`                 UNION ALL                 SELECT DISTINCT                     DataAreaId AS CompanyId,                     ItemId,                     UPPER(SalesUnit) AS FromUOM                 FROM `YOUR_CATALOG`.`YOUR_SCHEMA`.`SMRBICustPackingSlipTransStaging`             ) A             CROSS JOIN (                 SELECT 'PCS'   AS ToUOM UNION ALL                 SELECT 'BOX'   AS ToUOM UNION ALL                 SELECT 'PAL'   AS ToUOM             ) B             CROSS JOIN `YOUR_CATALOG`.`YOUR_SCHEMA`.`SMRBIOfficeAddinLegalEntityStaging` C             WHERE 1=1               AND NULLIF(ItemNumber, '') IS NOT NULL               AND FromUOM <> ToUOM              UNION ALL              SELECT DISTINCT                 C.CompanyId,                 A.ItemNumber,                 B.FromUOM,                 A.ToUOM             FROM (                 SELECT DISTINCT                     DataAreaId AS CompanyId,                     ItemNumber,                     UPPER(SalesUnitSymbol) AS ToUOM                 FROM `YOUR_CATALOG`.`YOUR_SCHEMA`.`SMRBISalesOrderLineStaging`                 UNION ALL                 SELECT DISTINCT                     DataAreaId AS CompanyId,                     ItemId,                     UPPER(SalesUnit) AS ToUOM                 FROM `YOUR_CATALOG`.`YOUR_SCHEMA`.`SMRBICustInvoiceTransStaging`                 UNION ALL                 SELECT DISTINCT                     DataAreaId AS CompanyId,                     ItemId,                     UPPER(SalesUnit) AS ToUOM                 FROM `YOUR_CATALOG`.`YOUR_SCHEMA`.`SMRBICustPackingSlipTransStaging`             ) A             CROSS JOIN (                 SELECT 'PCS'   AS FromUOM UNION ALL                 SELECT 'BOX'   AS FromUOM UNION ALL                 SELECT 'PAL'   AS FromUOM             ) B             CROSS JOIN `YOUR_CATALOG`.`YOUR_SCHEMA`.`SMRBIOfficeAddinLegalEntityStaging` C             WHERE 1=1               AND NULLIF(ItemNumber, '') IS NOT NULL               AND FromUOM <> ToUOM         ) C         LEFT JOIN `YOUR_CATALOG`.`YOUR_SCHEMA`.`SMRBIEcoResProductSpecificUnitOfMeasureConversionStaging` CONV             ON CONV.ProductNumber = C.ItemNumber            AND CONV.FromUnitSymbol = C.FromUOM            AND CONV.ToUnitSymbol   = C.ToUOM         LEFT JOIN `YOUR_CATALOG`.`YOUR_SCHEMA`.`SMRBIEcoResProductSpecificUnitOfMeasureConversionStaging` CONV2             ON CONV2.ProductNumber = C.ItemNumber            AND CONV2.FromUnitSymbol = C.ToUOM            AND CONV2.ToUnitSymbol   = C.FromUOM         WHERE 1=1           AND CONV.Factor IS NULL     ) D     LEFT JOIN `YOUR_CATALOG`.`YOUR_SCHEMA`.`SMRBIEcoResReleasedProductStaging` ERRPS         ON ERRPS.DataAreaId = D.CompanyId        AND ERRPS.ItemNumber = D.ItemNumber     LEFT JOIN `YOUR_CATALOG`.`YOUR_SCHEMA`.`SMRBIEcoResProductSpecificUnitOfMeasureConversionStaging` CONV3         ON CONV3.FromUnitSymbol = D.FromUOM        AND CONV3.ToUnitSymbol   = ERRPS.SalesUnitSymbol        AND CONV3.ProductNumber = D.ItemNumber     LEFT JOIN `YOUR_CATALOG`.`YOUR_SCHEMA`.`SMRBIEcoResProductSpecificUnitOfMeasureConversionStaging` CONV4         ON CONV4.FromUnitSymbol = D.ToUOM        AND CONV4.ToUnitSymbol   = ERRPS.SalesUnitSymbol        AND CONV4.ProductNumber = D.ItemNumber     LEFT JOIN (         SELECT ProductNumber,                ToUnitSymbol         FROM (             SELECT                 ROW_NUMBER() OVER (PARTITION BY ProductNumber                                    ORDER BY Nbr DESC,                                             CASE WHEN ToUnitSymbol = 'SU' THEN 1 ELSE 99 END) AS RankNr,                 ProductNumber,                 ToUnitSymbol             FROM (                 SELECT                     ProductNumber,                     ToUnitSymbol,                     COUNT(*) AS Nbr                 FROM `YOUR_CATALOG`.`YOUR_SCHEMA`.`SMRBIEcoResProductSpecificUnitOfMeasureConversionStaging`                 GROUP BY ProductNumber, ToUnitSymbol             ) A         ) A         GROUP BY RankNr, ProductNumber, ToUnitSymbol         HAVING RankNr = 1     ) TMP         ON ERRPS.ItemNumber = TMP.ProductNumber     LEFT JOIN `YOUR_CATALOG`.`YOUR_SCHEMA`.`SMRBIEcoResProductSpecificUnitOfMeasureConversionStaging` CONV5         ON CONV5.FromUnitSymbol = D.FromUOM        AND CONV5.ToUnitSymbol   = TMP.ToUnitSymbol        AND CONV5.ProductNumber = D.ItemNumber     LEFT JOIN `YOUR_CATALOG`.`YOUR_SCHEMA`.`SMRBIEcoResProductSpecificUnitOfMeasureConversionStaging` CONV6         ON CONV6.FromUnitSymbol = D.ToUOM        AND CONV6.ToUnitSymbol   = TMP.ToUnitSymbol        AND CONV6.ProductNumber = D.ItemNumber     WHERE 1=1       AND CONV.Factor / CONV2.Factor IS NOT NULL ),  Result AS (     SELECT DISTINCT         UOMC.Denominator   AS Denominator,         CAST('Normal' AS STRING) AS DataFlow,         CAST(UOMC.Factor AS DECIMAL(32,17)) AS Factor,         UOMC.InnerOffset  AS InnerOffset,         UOMC.OuterOffset  AS OuterOffset,         UOMC.ProductNumber AS Product,         COALESCE(ERRPS.ItemNumber, '_N/A') AS ItemNumber,         UOMC.Rounding     AS Rounding,         UPPER(UOMC.FromUnitSymbol) AS FromUOM,         UPPER(UOMC.ToUnitSymbol)   AS ToUOM,         COALESCE(ERRPS.DataAreaId, '_N/A') AS CompanyCode     FROM `YOUR_CATALOG`.`YOUR_SCHEMA`.`SMRBIEcoResProductSpecificUnitOfMeasureConversionStaging` UOMC     LEFT JOIN `YOUR_CATALOG`.`YOUR_SCHEMA`.`SMRBIEcoResReleasedProductStaging` ERRPS           ON ERRPS.ProductNumber = UOMC.ProductNumber      UNION ALL      SELECT         UOMC.Denominator   AS Denominator,         CAST('Normal|Non-Item' AS STRING) AS DataFlow,         CAST(UOMC.Factor AS DECIMAL(32,17)) AS Factor,         UOMC.InnerOffset  AS InnerOffset,         UOMC.OuterOffset  AS OuterOffset,         'NON-ITEM DRIVEN' AS Product,         'NON-ITEM DRIVEN' AS ItemNumber,         UOMC.Rounding     AS Rounding,         UPPER(UOMC.FromUnitSymbol) AS FromUOM,         UPPER(UOMC.ToUnitSymbol)   AS ToUOM,         COALESCE(OALES.CompanyId, '_N/A') AS CompanyCode     FROM `YOUR_CATALOG`.`YOUR_SCHEMA`.`SMRBIUnitOfMeasureConversionStaging` UOMC     CROSS JOIN `YOUR_CATALOG`.`YOUR_SCHEMA`.`SMRBIOfficeAddinLegalEntityStaging` OALES      UNION ALL      SELECT         UOMC.Denominator   AS Denominator,         CAST('Normal (Rev)' AS STRING) AS DataFlow,         ISNULL(1 / NULLIF(UOMC.Factor,0), 0)       AS Factor,         ISNULL(1 / NULLIF(UOMC.InnerOffset,0), 0) AS InnerOffset,         ISNULL(1 / NULLIF(UOMC.OuterOffset,0), 0) AS OuterOffset,         UOMC.ProductNumber AS Product,         COALESCE(ERRPS.ItemNumber, '_N/A') AS ItemNumber,         UOMC.Rounding     AS Rounding,         UPPER(UOMC.ToUnitSymbol) AS FromUOM,         UPPER(UOMC.FromUnitSymbol) AS ToUOM,         COALESCE(ERRPS.DataAreaId, '_N/A') AS CompanyCode     FROM `YOUR_CATALOG`.`YOUR_SCHEMA`.`SMRBIEcoResProductSpecificUnitOfMeasureConversionStaging` UOMC     LEFT JOIN `YOUR_CATALOG`.`YOUR_SCHEMA`.`SMRBIEcoResReleasedProductStaging` ERRPS           ON ERRPS.ProductNumber = UOMC.ProductNumber      UNION ALL      SELECT         UOMC.Denominator   AS Denominator,         CAST('Normal|Non-Item' AS STRING) AS DataFlow,         ISNULL(1 / NULLIF(UOMC.Factor,0), 0)       AS Factor,         UOMC.InnerOffset   AS InnerOffset,         UOMC.OuterOffset   AS OuterOffset,         'NON-ITEM DRIVEN'   AS Product,         'NON-ITEM DRIVEN'   AS ItemNumber,         UOMC.Rounding      AS Rounding,         UPPER(UOMC.ToUnitSymbol) AS FromUOM,         UPPER(UOMC.FromUnitSymbol) AS ToUOM,         COALESCE(OALES.CompanyId, '_N/A') AS CompanyCode     FROM `YOUR_CATALOG`.`YOUR_SCHEMA`.`SMRBIUnitOfMeasureConversionStaging` UOMC     CROSS JOIN `YOUR_CATALOG`.`YOUR_SCHEMA`.`SMRBIOfficeAddinLegalEntityStaging` OALES      UNION ALL      SELECT         1 AS Denominator,         CAST('Manual' AS STRING) AS DataFlow,         CAST(RUC.Factor AS DECIMAL(32,17)) AS Factor,         0 AS InnerOffset,         0 AS OuterOffset,         RUC.ItemNumber AS Product,         RUC.ItemNumber AS ItemNumber,         1 AS Rounding,         RUC.FromUOM AS FromUOM,         RUC.ToUOM AS ToUOM,         RUC.CompanyId AS CompanyCode     FROM `YOUR_CATALOG`.`YOUR_SCHEMA`.`RequiredUnitConversions` RUC      UNION ALL      SELECT         -1 AS Denominator,         CAST('Dummy' AS STRING) AS DataFlow,         -1 AS Factor,         -1 AS InnerOffset,         -1 AS OuterOffset,         '_N/A' AS Product,         '_N/A' AS ItemNumber,         -1 AS Rounding,         '_N/A' AS FromUOM,         '_N/A' AS ToUOM,         UPPER(SM.CompanyId) AS CompanyCode   -- Dummy row for completeness     FROM `YOUR_CATALOG`.`YOUR_SCHEMA`.`SMRBIOfficeAddinLegalEntityStaging` SM )  SELECT * FROM Result;
# MAGIC ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
