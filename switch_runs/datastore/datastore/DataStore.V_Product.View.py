# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_Product.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_Product.View.sql`

# COMMAND ----------

# --------------------------------------------------------------------------------
#  Create a Databricks view that mirrors the Transact‑SQL view ``[DataStore].[V_Product]``.
#
#  * All catalog and schema references are fully qualified: dbe_dbx_internships.datastore.table
#  * T‑SQL functions that do not exist in Spark are replaced with their
#    Spark SQL equivalents (ISNULL → coalesce, NULLIF, etc.).
#  * The ``PIVOT`` clause used in the original script is expressed with
#    conditional aggregation, which Spark SQL supports.
#  * All identifiers that were originally wrapped in brackets or quotes are
#    written without brackets – Spark SQL does not require them.
#  * The original T‑SQL uses a ``UNIQUEIDENTIFIER`` primary key and other
#    types; they are left unchanged because the view does not expose these
#    underlying tables directly – only their derived columns are exposed.
#
#  Note:  If any of the referenced base tables do not exist in the Databricks
#  catalogue, the view creation will fail with a “table not found” error.
#  Make sure the tables are already present in the same database before
#  running this notebook.
# --------------------------------------------------------------------------------

spark.sql(f"""
-- --------------------------------------------------------------------------
-- 1️⃣ Create or replace the view `V_Product` in the target database.
-- --------------------------------------------------------------------------
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.`V_Product` AS

/* ------------- 1️⃣ The final SELECT clause ---------------------------- */
SELECT
    ProductId,
    CompanyId AS CompanyCode,
    ProductCode,
    ProductName,
    ProductGroupCode,
    ProductGroupName,
    ProductGroupCodeName,
    ProductInventoryUnit,
    ProductPurchaseUnit,
    ProductSalesUnit,
    PhysicalUnitSymbol,
    PhysicalVolume,
    PhysicalWeight,
    PrimaryVendorId AS PrimaryVendorCode,
    CountryOfOrigin,
    IntrastatCommodityCode,
    ABCClassification,
    Brand,
    Material,
    BusinessType

/* ------------- 2️⃣ Materialised view consisting of two CTEs ------------- */
FROM (

    /* CTE 2️⃣ – Aggregate product details and attribute values. */
    SELECT
        ProductId,
        CompanyId,
        ProductCode,
        ProductName,
        ProductGroupCode,
        ProductGroupName,
        ProductGroupCodeName,
        ProductInventoryUnit,
        ProductPurchaseUnit,
        PhysicalUnitSymbol,
        PhysicalVolume,
        PhysicalWeight,
        ProductSalesUnit,
        PrimaryVendorId,
        CountryOfOrigin,
        IntrastatCommodityCode,
        ABCClassification,

        /* 3️⃣ Brand, Material and BusinessType are aggregated from the
           * underlying attribute rows using conditional aggregation.
           * ``coalesce`` handles NULL and empty string values. */
        COALESCE(NULLIF(MIN(CASE WHEN AttributeName = 'Brand' THEN AttributeValue END), ''), '_N/A')      AS Brand,
        COALESCE(NULLIF(MIN(CASE WHEN AttributeName = 'Material' THEN AttributeValue END), ''), '_N/A')   AS Material,
        COALESCE(NULLIF(MIN(CASE WHEN AttributeName = 'BusinessType' THEN AttributeValue END), ''), '_N/A') AS BusinessType

    FROM (

        /* CTE 1️⃣ – Base product information joined to multiple
           * reference tables. */
        SELECT
            ERRPS.ProductRecId                                    AS ProductId,
            UPPER(ERRPS.DataAreaId)                              AS CompanyId,
            COALESCE(NULLIF(UPPER(ERRPS.ItemNumber), ''), '_N/A') AS ProductCode,
            COALESCE(NULLIF(ERPTS.ProductName, ''), '_N/A')       AS ProductName,
            COALESCE(NULLIF(ERRPS.ProductGroupId, ''), '_N/A')    AS ProductGroupCode,
            COALESCE(CASE WHEN IPGS.Name = '' THEN NULL ELSE IPGS.Name END, '_N/A') AS ProductGroupName,
            COALESCE((ERRPS.ProductGroupId || ' - ' ||
                      COALESCE(CASE WHEN IPGS.Name = '' THEN NULL ELSE IPGS.Name END, '_N/A')),
                      '_N/A')                                               AS ProductGroupCodeName,
            COALESCE(NULLIF(UPPER(ERRPS.InventoryUnitSymbol), ''), '_N/A') AS ProductInventoryUnit,
            COALESCE(NULLIF(UPPER(ERRPS.PurchaseUnitSymbol), ''), '_N/A')   AS ProductPurchaseUnit,
            COALESCE(NULLIF(UPPER(ERRPS.SalesUnitSymbol), ''), '_N/A')     AS ProductSalesUnit,
            COALESCE(PDGD.PhysicalDepth, 0) *
            COALESCE(PDGD.PhysicalHeight, 0) *
            COALESCE(PDGD.PhysicalWidth, 0)                                AS PhysicalVolume,
            COALESCE(PDGD.PhysicalWeight, 0)                            AS PhysicalWeight,
            COALESCE(PDGD.PhysicalUnitSymbol, '_N/A')                     AS PhysicalUnitSymbol,
            COALESCE(NULLIF(PPAVS.ApprovedVendorAccountNumber, ''), NULLIF(ERRPS.PrimaryVendorAccountNumber, ''), '_N/A')
                                                                                        AS PrimaryVendorId,
            COALESCE(NULLIF(ERRPS.OriginCountryRegionId, ''), '_N/A')      AS CountryOfOrigin,
            COALESCE(NULLIF(ERRPS.IntrastatCommodityCode, ''), '_N/A')     AS IntrastatCommodityCode,

            /* Revenue ABC classification – convert numeric codes to
               * string values. */
            CAST(CASE
                    WHEN COALESCE(NULLIF(ERRPS.RevenueAbcCode, ''), '0') = '0' THEN 'None'
                    WHEN COALESCE(NULLIF(ERRPS.RevenueAbcCode, ''), '0') = '1' THEN 'A'
                    WHEN COALESCE(NULLIF(ERRPS.RevenueAbcCode, ''), '0') = '2' THEN 'B'
                    WHEN COALESCE(NULLIF(ERRPS.RevenueAbcCode, ''), '0') = '3' THEN 'C'
                 END AS STRING)                                          AS ABCClassification,

            /* AttributeName and AttributeValue are returned in one row
               * per attribute.  These values are later aggregated
               * in the outer SELECT. */
            P.AttributeName,
            P.AttributeValue

        FROM dbe_dbx_internships.datastore.SMRBIEcoResReleasedProductStaging ERRPS

        /* -----------------------------------------------------------------
           Join to vendor approval table – uses a window function to keep
           only the first (rank 1) record per (DataAreaId, ItemNumber). */
        LEFT JOIN (
            SELECT
                ROW_NUMBER() OVER (PARTITION BY PPAVS.DataAreaId, PPAVS.ItemNumber
                                   ORDER BY PPAVS.DataAreaId, PPAVS.ItemNumber) AS RankNr,
                PPAVS.DataAreaId,
                PPAVS.ItemNumber,
                PPAVS.ApprovedVendorAccountNumber,
                VVS.VendorOrganizationName
            FROM dbe_dbx_internships.datastore.SMRBIPurchProductApprovedVendorStaging PPAVS
            LEFT JOIN dbe_dbx_internships.datastore.SMRBIVendorStaging VVS
              ON VVS.DataAreaId = PPAVS.DataAreaId
              AND VVS.VendorAccountNumber = PPAVS.ApprovedVendorAccountNumber
        ) PPAVS
            ON PPAVS.DataAreaId = ERRPS.DataAreaId
           AND PPAVS.ItemNumber = ERRPS.ItemNumber
           AND PPAVS.RankNr = 1

        /* -----------------------------------------------------------------
           In‑house product dimension table – provides physical volume/weight. */
        LEFT JOIN dbe_dbx_internships.datastore.SMRBIWHSPhysicalDimensionGroupDetailStaging PDGD
           ON ERRPS.PhysicalDimensionGroupId = PDGD.PhysicalDimensionGroupId
          AND ERRPS.DataAreaId = PDGD.DataAreaId
          AND ERRPS.InventoryUnitSymbol = PDGD.PhysicalUnitSymbol

        /* -----------------------------------------------------------------
           Language specific product translation – English only. */
        LEFT JOIN dbe_dbx_internships.datastore.SMRBIEcoResProductTranslationStaging ERPTS
           ON ERRPS.ProductNumber = ERPTS.ProductNumber
          AND ERPTS.LanguageId = 'en-us'

        /* -----------------------------------------------------------------
           Product grouping information. */
        LEFT JOIN dbe_dbx_internships.datastore.SMRBIInventProductGroupStaging IPGS
           ON ERRPS.ProductGroupId = IPGS.ItemGroupId
          AND ERRPS.DataAreaId = IPGS.DataAreaId

        /* -----------------------------------------------------------------
           Attribute values – the original PIVOT clause is replaced by a
           "derived" sub‑query which returns one row per (ProductNumber,
           AttributeName, AttributeValue).  The outer SELECT will
           aggregate these rows. */
        LEFT JOIN (
            SELECT
                COALESCE(ERPAVS1.ProductNumber,
                         ERPAVS2.ProductNumber,
                         ERPAVS3.ProductNumber,
                         ERPAVS4.ProductNumber,
                         ERPAVS5.ProductNumber,
                         ERPAVS20.ProductNumber)         AS ProductNumber,
                COALESCE(ERPAVS1.AttributeName,
                         ERPAVS2.AttributeName,
                         ERPAVS3.AttributeName,
                         ERPAVS4.AttributeName,
                         ERPAVS5.AttributeName,
                         ERPAVS20.AttributeName)        AS AttributeName,
                CASE
                    WHEN ERPAVS.DataType = 1 THEN CAST(ERPAVS1.CurrencyCode AS STRING)
                    WHEN ERPAVS.DataType = 2 THEN CAST(ERPAVS2.DateTimeValue AS STRING)
                    WHEN ERPAVS.DataType = 3 THEN CAST(ERPAVS3.DecimalValue AS STRING)
                    WHEN ERPAVS.DataType = 4 THEN CAST(ERPAVS4.IntegerValue AS STRING)
                    WHEN ERPAVS.DataType = 5 THEN CAST(ERPAVS5.TextValue AS STRING)
                    WHEN ERPAVS.DataType = 20 THEN CAST(ERPAVS20.BooleanValue AS STRING)
                END AS AttributeValue
            FROM dbe_dbx_internships.datastore.SMRBIEcoResProductAttributeValueStaging ERPAVS

            LEFT JOIN dbe_dbx_internships.datastore.SMRBIEcoResProductAttributeValueStaging ERPAVS1
               ON ERPAVS.Name = ERPAVS1.AttributeTypeName
              AND ERPAVS.ProductNumber = ERPAVS1.ProductNumber
              AND ERPAVS.DataType = 1

            LEFT JOIN dbe_dbx_internships.datastore.SMRBIEcoResProductAttributeValueStaging ERPAVS2
               ON ERPAVS.Name = ERPAVS2.AttributeTypeName
              AND ERPAVS.ProductNumber = ERPAVS2.ProductNumber
              AND ERPAVS.DataType = 2

            LEFT JOIN dbe_dbx_internships.datastore.SMRBIEcoResProductAttributeValueStaging ERPAVS3
               ON ERPAVS.Name = ERPAVS3.AttributeTypeName
              AND ERPAVS.ProductNumber = ERPAVS3.ProductNumber
              AND ERPAVS.DataType = 3

            LEFT JOIN dbe_dbx_internships.datastore.SMRBIEcoResProductAttributeValueStaging ERPAVS4
               ON ERPAVS.Name = ERPAVS4.AttributeTypeName
              AND ERPAVS.ProductNumber = ERPAVS4.ProductNumber
              AND ERPAVS.DataType = 4

            LEFT JOIN dbe_dbx_internships.datastore.SMRBIEcoResProductAttributeValueStaging ERPAVS5
               ON ERPAVS.Name = ERPAVS5.AttributeTypeName
              AND ERPAVS.ProductNumber = ERPAVS5.ProductNumber
              AND ERPAVS.DataType = 5

            LEFT JOIN dbe_dbx_internships.datastore.SMRBIEcoResProductAttributeValueStaging ERPAVS20
               ON ERPAVS.Name = ERPAVS20.AttributeTypeName
              AND ERPAVS.ProductNumber = ERPAVS20.ProductNumber
              AND ERPAVS.DataType = 20

            WHERE COALESCE(ERPAVS1.ProductNumber,
                           ERPAVS2.ProductNumber,
                           ERPAVS3.ProductNumber,
                           ERPAVS4.ProductNumber,
                           ERPAVS5.ProductNumber,
                           ERPAVS20.ProductNumber) IS NOT NULL
        ) P                               /* Pivot‑equivalent alias */
            ON P.ProductNumber = ERRPS.ItemNumber

    ) Derived                   /* End of CTE 1️⃣ */
    GROUP BY
        ProductId,
        CompanyId,
        ProductCode,
        ProductName,
        ProductGroupCode,
        ProductGroupName,
        ProductGroupCodeName,
        ProductInventoryUnit,
        ProductPurchaseUnit,
        PhysicalUnitSymbol,
        PhysicalVolume,
        PhysicalWeight,
        ProductSalesUnit,
        PrimaryVendorId,
        CountryOfOrigin,
        IntrastatCommodityCode,
        ABCClassification
) Aggregated                     /* End of CTE 2️⃣ */ ;
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 11226)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN -- -------------------------------------------------------------------------- -- 1️⃣ Create or replace the view `V_Product` in the target database. -- -------------------------------------------------------------------------- CREATE OR REPLACE VIEW `_placeholder_`.`_placeholder_`.`V_Product` AS  /* ------------- 1️⃣ The final SELECT clause ---------------------------- */ SELECT     ProductId,     CompanyId AS CompanyCode,     ProductCode,     ProductName,     ProductGroupCode,     ProductGroupName,     ProductGroupCodeName,     ProductInventoryUnit,     ProductPurchaseUnit,     ProductSalesUnit,     PhysicalUnitSymbol,     PhysicalVolume,     PhysicalWeight,     PrimaryVendorId AS PrimaryVendorCode,     CountryOfOrigin,     IntrastatCommodityCode,     ABCClassification,     Brand,     Material,     BusinessType  /* ------------- 2️⃣ Materialised view consisting of two CTEs ------------- */ FROM (      /* CTE 2️⃣ – Aggregate product details and attribute values. */     SELECT         ProductId,         CompanyId,         ProductCode,         ProductName,         ProductGroupCode,         ProductGroupName,         ProductGroupCodeName,         ProductInventoryUnit,         ProductPurchaseUnit,         PhysicalUnitSymbol,         PhysicalVolume,         PhysicalWeight,         ProductSalesUnit,         PrimaryVendorId,         CountryOfOrigin,         IntrastatCommodityCode,         ABCClassification,          /* 3️⃣ Brand, Material and BusinessType are aggregated from the            * underlying attribute rows using conditional aggregation.            * ``coalesce`` handles NULL and empty string values. */         COALESCE(NULLIF(MIN(CASE WHEN AttributeName = 'Brand' THEN AttributeValue END), ''), '_N/A')      AS Brand,         COALESCE(NULLIF(MIN(CASE WHEN AttributeName = 'Material' THEN AttributeValue END), ''), '_N/A')   AS Material,         COALESCE(NULLIF(MIN(CASE WHEN AttributeName = 'BusinessType' THEN AttributeValue END), ''), '_N/A') AS BusinessType      FROM (          /* CTE 1️⃣ – Base product information joined to multiple            * reference tables. */         SELECT             ERRPS.ProductRecId                                    AS ProductId,             UPPER(ERRPS.DataAreaId)                              AS CompanyId,             COALESCE(NULLIF(UPPER(ERRPS.ItemNumber), ''), '_N/A') AS ProductCode,             COALESCE(NULLIF(ERPTS.ProductName, ''), '_N/A')       AS ProductName,             COALESCE(NULLIF(ERRPS.ProductGroupId, ''), '_N/A')    AS ProductGroupCode,             COALESCE(CASE WHEN IPGS.Name = '' THEN NULL ELSE IPGS.Name END, '_N/A') AS ProductGroupName,             COALESCE((ERRPS.ProductGroupId || ' - ' ||                       COALESCE(CASE WHEN IPGS.Name = '' THEN NULL ELSE IPGS.Name END, '_N/A')),                       '_N/A')                                               AS ProductGroupCodeName,             COALESCE(NULLIF(UPPER(ERRPS.InventoryUnitSymbol), ''), '_N/A') AS ProductInventoryUnit,             COALESCE(NULLIF(UPPER(ERRPS.PurchaseUnitSymbol), ''), '_N/A')   AS ProductPurchaseUnit,             COALESCE(NULLIF(UPPER(ERRPS.SalesUnitSymbol), ''), '_N/A')     AS ProductSalesUnit,             COALESCE(PDGD.PhysicalDepth, 0) *             COALESCE(PDGD.PhysicalHeight, 0) *             COALESCE(PDGD.PhysicalWidth, 0)                                AS PhysicalVolume,             COALESCE(PDGD.PhysicalWeight, 0)                            AS PhysicalWeight,             COALESCE(PDGD.PhysicalUnitSymbol, '_N/A')                     AS PhysicalUnitSymbol,             COALESCE(NULLIF(PPAVS.ApprovedVendorAccountNumber, ''), NULLIF(ERRPS.PrimaryVendorAccountNumber, ''), '_N/A')                                                                                         AS PrimaryVendorId,             COALESCE(NULLIF(ERRPS.OriginCountryRegionId, ''), '_N/A')      AS CountryOfOrigin,             COALESCE(NULLIF(ERRPS.IntrastatCommodityCode, ''), '_N/A')     AS IntrastatCommodityCode,              /* Revenue ABC classification – convert numeric codes to                * string values. */             CAST(CASE                     WHEN COALESCE(NULLIF(ERRPS.RevenueAbcCode, ''), '0') = '0' THEN 'None'                     WHEN COALESCE(NULLIF(ERRPS.RevenueAbcCode, ''), '0') = '1' THEN 'A'                     WHEN COALESCE(NULLIF(ERRPS.RevenueAbcCode, ''), '0') = '2' THEN 'B'                     WHEN COALESCE(NULLIF(ERRPS.RevenueAbcCode, ''), '0') = '3' THEN 'C'                  END AS STRING)                                          AS ABCClassification,              /* AttributeName and AttributeValue are returned in one row                * per attribute.  These values are later aggregated                * in the outer SELECT. */             P.AttributeName,             P.AttributeValue          FROM _placeholder_._placeholder_.SMRBIEcoResReleasedProductStaging ERRPS          /* -----------------------------------------------------------------            Join to vendor approval table – uses a window function to keep            only the first (rank 1) record per (DataAreaId, ItemNumber). */         LEFT JOIN (             SELECT                 ROW_NUMBER() OVER (PARTITION BY PPAVS.DataAreaId, PPAVS.ItemNumber                                    ORDER BY PPAVS.DataAreaId, PPAVS.ItemNumber) AS RankNr,                 PPAVS.DataAreaId,                 PPAVS.ItemNumber,                 PPAVS.ApprovedVendorAccountNumber,                 VVS.VendorOrganizationName             FROM _placeholder_._placeholder_.SMRBIPurchProductApprovedVendorStaging PPAVS             LEFT JOIN _placeholder_._placeholder_.SMRBIVendorStaging VVS               ON VVS.DataAreaId = PPAVS.DataAreaId               AND VVS.VendorAccountNumber = PPAVS.ApprovedVendorAccountNumber         ) PPAVS             ON PPAVS.DataAreaId = ERRPS.DataAreaId            AND PPAVS.ItemNumber = ERRPS.ItemNumber            AND PPAVS.RankNr = 1          /* -----------------------------------------------------------------            In‑house product dimension table – provides physical volume/weight. */         LEFT JOIN _placeholder_._placeholder_.SMRBIWHSPhysicalDimensionGroupDetailStaging PDGD            ON ERRPS.PhysicalDimensionGroupId = PDGD.PhysicalDimensionGroupId           AND ERRPS.DataAreaId = PDGD.DataAreaId           AND ERRPS.InventoryUnitSymbol = PDGD.PhysicalUnitSymbol          /* -----------------------------------------------------------------            Language specific product translation – English only. */         LEFT JOIN _placeholder_._placeholder_.SMRBIEcoResProductTranslationStaging ERPTS            ON ERRPS.ProductNumber = ERPTS.ProductNumber           AND ERPTS.LanguageId = 'en-us'          /* -----------------------------------------------------------------            Product grouping information. */         LEFT JOIN _placeholder_._placeholder_.SMRBIInventProductGroupStaging IPGS            ON ERRPS.ProductGroupId = IPGS.ItemGroupId           AND ERRPS.DataAreaId = IPGS.DataAreaId          /* -----------------------------------------------------------------            Attribute values – the original PIVOT clause is replaced by a            "derived" sub‑query which returns one row per (ProductNumber,            AttributeName, AttributeValue).  The outer SELECT will            aggregate these rows. */         LEFT JOIN (             SELECT                 COALESCE(ERPAVS1.ProductNumber,                          ERPAVS2.ProductNumber,                          ERPAVS3.ProductNumber,                          ERPAVS4.ProductNumber,                          ERPAVS5.ProductNumber,                          ERPAVS20.ProductNumber)         AS ProductNumber,                 COALESCE(ERPAVS1.AttributeName,                          ERPAVS2.AttributeName,                          ERPAVS3.AttributeName,                          ERPAVS4.AttributeName,                          ERPAVS5.AttributeName,                          ERPAVS20.AttributeName)        AS AttributeName,                 CASE                     WHEN ERPAVS.DataType = 1 THEN CAST(ERPAVS1.CurrencyCode AS STRING)                     WHEN ERPAVS.DataType = 2 THEN CAST(ERPAVS2.DateTimeValue AS STRING)                     WHEN ERPAVS.DataType = 3 THEN CAST(ERPAVS3.DecimalValue AS STRING)                     WHEN ERPAVS.DataType = 4 THEN CAST(ERPAVS4.IntegerValue AS STRING)                     WHEN ERPAVS.DataType = 5 THEN CAST(ERPAVS5.TextValue AS STRING)                     WHEN ERPAVS.DataType = 20 THEN CAST(ERPAVS20.BooleanValue AS STRING)                 END AS AttributeValue             FROM _placeholder_._placeholder_.SMRBIEcoResProductAttributeValueStaging ERPAVS              LEFT JOIN _placeholder_._placeholder_.SMRBIEcoResProductAttributeValueStaging ERPAVS1                ON ERPAVS.Name = ERPAVS1.AttributeTypeName               AND ERPAVS.ProductNumber = ERPAVS1.ProductNumber               AND ERPAVS.DataType = 1              LEFT JOIN _placeholder_._placeholder_.SMRBIEcoResProductAttributeValueStaging ERPAVS2                ON ERPAVS.Name = ERPAVS2.AttributeTypeName               AND ERPAVS.ProductNumber = ERPAVS2.ProductNumber               AND ERPAVS.DataType = 2              LEFT JOIN _placeholder_._placeholder_.SMRBIEcoResProductAttributeValueStaging ERPAVS3                ON ERPAVS.Name = ERPAVS3.AttributeTypeName               AND ERPAVS.ProductNumber = ERPAVS3.ProductNumber               AND ERPAVS.DataType = 3              LEFT JOIN _placeholder_._placeholder_.SMRBIEcoResProductAttributeValueStaging ERPAVS4                ON ERPAVS.Name = ERPAVS4.AttributeTypeName               AND ERPAVS.ProductNumber = ERPAVS4.ProductNumber               AND ERPAVS.DataType = 4              LEFT JOIN _placeholder_._placeholder_.SMRBIEcoResProductAttributeValueStaging ERPAVS5                ON ERPAVS.Name = ERPAVS5.AttributeTypeName               AND ERPAVS.ProductNumber = ERPAVS5.ProductNumber               AND ERPAVS.DataType = 5              LEFT JOIN _placeholder_._placeholder_.SMRBIEcoResProductAttributeValueStaging ERPAVS20                ON ERPAVS.Name = ERPAVS20.AttributeTypeName               AND ERPAVS.ProductNumber = ERPAVS20.ProductNumber               AND ERPAVS.DataType = 20              WHERE COALESCE(ERPAVS1.ProductNumber,                            ERPAVS2.ProductNumber,                            ERPAVS3.ProductNumber,                            ERPAVS4.ProductNumber,                            ERPAVS5.ProductNumber,                            ERPAVS20.ProductNumber) IS NOT NULL         ) P                               /* Pivot‑equivalent alias */             ON P.ProductNumber = ERRPS.ItemNumber      ) Derived                   /* End of CTE 1️⃣ */     GROUP BY         ProductId,         CompanyId,         ProductCode,         ProductName,         ProductGroupCode,         ProductGroupName,         ProductGroupCodeName,         ProductInventoryUnit,         ProductPurchaseUnit,         PhysicalUnitSymbol,         PhysicalVolume,         PhysicalWeight,         ProductSalesUnit,         PrimaryVendorId,         CountryOfOrigin,         IntrastatCommodityCode,         ABCClassification ) Aggregated                     /* End of CTE 2️⃣ */ ;
# MAGIC ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
