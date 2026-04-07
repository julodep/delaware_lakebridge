# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_SalesInvoice.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_SalesInvoice.View.sql`

# COMMAND ----------

# Create or replace the view
spark.sql(f"""
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.`V_SalesInvoice` AS

/* Inner query: build all columns from the staging tables and aggregate the necessary look‑ups. */
SELECT
    -- 1‑Basic identifiers
    SalesInvoiceId AS SalesInvoiceCode,
    TransactionType,
    SalesInvoiceLineNumber,
    SalesInvoiceLineNumberCombination,

    HeaderRecId,
    LineRecId,

    SalesInvoiceId AS SalesOrderCode,
    InventTransId AS InventTransCode,
    InventDimId AS InventDimCode,

    -- 2‑Financial amounts
    TaxWriteCode,
    SalesOrderStatus,
    CompanyId AS CompanyCode,
    ProductId AS ProductCode,
    OrderCustomerId AS OrderCustomerCode,
    InvoiceCustomerId AS CustomerCode,
    DeliveryModeId AS DeliveryModeCode,
    PaymentTermsId AS PaymentTermsCode,
    DeliveryTermsId AS DeliveryTermsCode,
    TransactionCurrencyId AS TransactionCurrencyCode,
    LedgerCode,
    OrigSalesOrderId,

    -- 3‑Dates & metrics
    InvoiceDate,
    RequestedDeliveryDate,
    ConfirmedDeliveryDate,
    SalesUnit,
    InvoicedQuantity,
    SalesPricePerUnitTC,
    GrossSalesTC,
    DiscountAmountTC,
    InvoicedSalesAmountTC,
    MarkupAmountTC,
    NetSalesTC

FROM
    /* Core data – flat table derived from the staging tables and markup look‑ups. */
    (
        SELECT
            -- Headers
            CIJS.InvoiceId                                   AS SalesInvoiceId,
            CASE
                WHEN LEFT(CIJS.InvoiceId, 2) = 'CI'     THEN 'Customer Invoice'
                WHEN LEFT(CIJS.InvoiceId, 3) = 'FTI'    THEN 'Customer Free Text Invoice'
                WHEN LEFT(CIJS.InvoiceId, 3) = 'SCN'    THEN 'Customer Credit Note'
                WHEN LEFT(CIJS.InvoiceId, 3) = 'FTC'    THEN 'Customer Free Text Credit Note'
                WHEN LEFT(CIJS.InvoiceId, 2) = 'RE'     THEN 'Customer Rebate'
                ELSE '_N/A'
            END                                           AS TransactionType,

            CITS.LineNum                                   AS SalesInvoiceLineNumber,
            CONCAT(UPPER(CIJS.InvoiceId), ' - ', CAST(CITS.LineNum AS STRING))
                                                    AS SalesInvoiceLineNumberCombination,

            CIJS.CustInvoiceJourRecId                     AS HeaderRecId,
            CITS.CustInvoiceTransRecId                    AS LineRecId,

            CASE WHEN CIJS.SalesId = '' THEN NULL ELSE CIJS.SalesId END
                                                    AS SalesOrderId,
            CASE WHEN CITS.InventTransId = '' THEN NULL ELSE CITS.InventTransId END
                                                    AS InventTransId,
            CITS.InventDimId                                 AS InventDimId,

            /* TAXWRITE logic – numeric prefix extraction. */
            CASE
                WHEN CITS.TaxWriteCode NOT IN ('21%', '12%', '6%') THEN 0
                WHEN CITS.TaxWriteCode = '6%'                     THEN 6
                ELSE
                    CASE
                        WHEN LENGTH(REPLACE(CITS.TaxWriteCode, ',', '')) >= 2
                        THEN CAST(SUBSTRING(REPLACE(CITS.TaxWriteCode, ',', ''), 1, 2) AS INT)
                        ELSE 0
                    END
            END                                         AS TaxWriteCode,

            'Invoiced'                                    AS SalesOrderStatus,
            UPPER(CIJS.DataAreaId)                        AS CompanyId,

            CASE WHEN UPPER(CITS.ItemId) = '' THEN NULL ELSE UPPER(CITS.ItemId) END
                                                    AS ProductId,
            CASE WHEN UPPER(CIJS.OrderAccount) = '' THEN NULL ELSE UPPER(CIJS.OrderAccount) END
                                                    AS OrderCustomerId,
            CASE WHEN CIJS.InvoiceAccount = '' THEN NULL ELSE CIJS.InvoiceAccount END
                                                    AS InvoiceCustomerId,

            CASE WHEN UPPER(CIJS.DlvMode) = '' THEN NULL ELSE UPPER(CIJS.DlvMode) END
                                                    AS DeliveryModeId,
            CASE WHEN UPPER(CIJS.Payment) = '' THEN NULL ELSE UPPER(CIJS.Payment) END
                                                    AS PaymentTermsId,
            CASE WHEN UPPER(CIJS.DlvTerm) = '' THEN NULL ELSE UPPER(CIJS.DlvTerm) END
                                                    AS DeliveryTermsId,

            UPPER(CITS.CurrencyCode)                     AS TransactionCurrencyId,
            CITS.LEDGERDIMENSIONDISPLAYVALUE            AS LedgerCode,

            CASE WHEN CITS.OrigSalesId = '' THEN NULL ELSE CITS.OrigSalesId END
                                                    AS OrigSalesOrderId,

            COALESCE(CAST(CIJS.InvoiceDate AS DATE), DATE('1900-01-01'))
                                                    AS InvoiceDate,
            DATE('1900-01-01')                           AS RequestedDeliveryDate,
            COALESCE(CAST(CITS.DLVDate AS DATE), DATE('1900-01-01'))
                                                    AS ConfirmedDeliveryDate,

            CASE WHEN CITS.SalesUnit = '' THEN NULL ELSE CITS.SalesUnit END
                                                    AS SalesUnit,
            COALESCE(CITS.Qty, 0)                        AS InvoicedQuantity,
            CITS.SalesPrice / CASE WHEN CITS.PriceUnit = 0 THEN 1 ELSE CITS.PriceUnit END
                                                    AS SalesPricePerUnitTC,

            CITS.LineAmount +
            (CITS.LinePercent/100.0 * (CITS.Qty * CITS.SalesPrice / CASE WHEN CITS.PriceUnit = 0 THEN 1 ELSE CITS.PriceUnit END))
            + CITS.LineDisc * CITS.Qty                 AS GrossSalesTC,

            (CITS.LinePercent/100.0 * (CITS.Qty * CITS.SalesPrice / CASE WHEN CITS.PriceUnit = 0 THEN 1 ELSE CITS.PriceUnit END))
            + CITS.LineDisc * CITS.Qty                 AS DiscountAmountTC,

            CITS.LineAmount                                 AS InvoicedSalesAmountTC,

            -- Mark‑up calculations
            COALESCE(
                CASE
                    WHEN COALESCE(MTS1.MarkupCategory, 0) = 0 THEN COALESCE(MTS1.Markup, 0)
                    WHEN MTS1.MarkupCategory = 1 THEN COALESCE(MTS1.Markup, 0) *
                                                    (CITS.SalesPrice / CASE WHEN CITS.PriceUnit = 0 THEN 1 ELSE CITS.PriceUnit END)
                    WHEN MTS1.MarkupCategory = 2 THEN COALESCE(MTS1.Markup, 0) / 100.0 *
                                                    (CITS.Qty * CITS.SalesPrice / CASE WHEN CITS.PriceUnit = 0 THEN 1 ELSE CITS.PriceUnit END)
                END, 0)
            +
            COALESCE(
                CASE
                    WHEN COALESCE(MTS2.MarkupCategory, 0) = 0 THEN COALESCE(MTS2.Markup, 0)
                    WHEN MTS2.MarkupCategory = 2 THEN COALESCE(MTS2.Markup, 0) / 100.0 *
                                                    (CITS.Qty * CITS.SalesPrice / CASE WHEN CITS.PriceUnit = 0 THEN 1 ELSE CITS.PriceUnit END)
                END, 0)                             AS MarkupAmountTC,

            -- Net sales
            CITS.LineAmount -
            (
                COALESCE(
                    CASE
                        WHEN COALESCE(MTS1.MarkupCategory, 0) = 0 THEN COALESCE(MTS1.Markup, 0)
                        WHEN MTS1.MarkupCategory = 1 THEN COALESCE(MTS1.Markup, 0) *
                                                    (CITS.SalesPrice / CASE WHEN CITS.PriceUnit = 0 THEN 1 ELSE CITS.PriceUnit END)
                        WHEN MTS1.MarkupCategory = 2 THEN COALESCE(MTS1.Markup, 0) / 100.0 *
                                                    (CITS.Qty * CITS.SalesPrice / CASE WHEN CITS.PriceUnit = 0 THEN 1 ELSE CITS.PriceUnit END)
                    END, 0)
                +
                COALESCE(
                    CASE
                        WHEN COALESCE(MTS2.MarkupCategory, 0) = 0 THEN COALESCE(MTS2.Markup, 0)
                        WHEN MTS2.MarkupCategory = 2 THEN COALESCE(MTS2.Markup, 0) / 100.0 *
                                                    (CITS.Qty * CITS.SalesPrice / CASE WHEN CITS.PriceUnit = 0 THEN 1 ELSE CITS.PriceUnit END)
                    END, 0)
            )                                               AS NetSalesTC
        FROM
            `dbe_dbx_internships`.`datastore`.SMRBICustInvoiceJourStaging AS CIJS
        JOIN
            `dbe_dbx_internships`.`datastore`.SMRBICustInvoiceTransStaging AS CITS
            ON CIJS.InvoiceId        = CITS.InvoiceId
           AND CIJS.DataAreaId       = CITS.DataAreaId
           AND CIJS.InvoiceDate      = CITS.InvoiceDate
           AND CIJS.SalesId          = CITS.SalesId
        LEFT JOIN
            (
                SELECT
                    DataAreaId,
                    MarkupCategory,
                    TransRecId,
                    MarkupCode,
                    SUM(`Value`) AS Markup
                FROM
                    `dbe_dbx_internships`.`datastore`.SMRBIMarkupTransStaging
                WHERE
                    TransTableId IN (
                        SELECT TableId
                        FROM `dbe_dbx_internships`.`datastore`.SqlDictionary
                        WHERE TableName = 'CustInvoiceTrans'
                    )
                GROUP BY
                    DataAreaId,
                    MarkupCategory,
                    TransRecId,
                    MarkupCode
            ) AS MTS1
            ON CITS.CustInvoiceTransRecId = MTS1.TransRecId
           AND CITS.DataAreaId            = MTS1.DataAreaId
        LEFT JOIN
            (
                SELECT
                    DataAreaId,
                    MarkupCategory,
                    TransRecId,
                    MarkupCode,
                    SUM(`Value`) AS Markup
                FROM
                    `dbe_dbx_internships`.`datastore`.SMRBIMarkupTransStaging
                WHERE
                    TransTableId IN (
                        SELECT TableId
                        FROM `dbe_dbx_internships`.`datastore`.SqlDictionary
                        WHERE TableName = 'CustInvoiceJour'
                    )
                GROUP BY
                    DataAreaId,
                    MarkupCategory,
                    TransRecId,
                    MarkupCode
            ) AS MTS2
            ON CIJS.CustInvoiceJourRecId = MTS2.TransRecId
           AND CIJS.DataAreaId          = MTS2.DataAreaId
           AND CITS.LineNum             = 1
    ) AS SI;
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC These are errors from static syntax checks. Manual corrections are required for these errors.
# MAGIC ### Spark SQL Syntax Errors
# MAGIC ```
# MAGIC Error in query 0: 
# MAGIC [PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601 (line 1, pos 10502)
# MAGIC
# MAGIC == SQL ==
# MAGIC EXPLAIN CREATE OR REPLACE VIEW `_placeholder_`.`_placeholder_`.`V_SalesInvoice` AS  /* Inner query: build all columns from the staging tables and aggregate the necessary look‑ups. */ SELECT     -- 1‑Basic identifiers     SalesInvoiceId AS SalesInvoiceCode,     TransactionType,     SalesInvoiceLineNumber,     SalesInvoiceLineNumberCombination,      HeaderRecId,     LineRecId,      SalesInvoiceId AS SalesOrderCode,     InventTransId AS InventTransCode,     InventDimId AS InventDimCode,      -- 2‑Financial amounts     TaxWriteCode,     SalesOrderStatus,     CompanyId AS CompanyCode,     ProductId AS ProductCode,     OrderCustomerId AS OrderCustomerCode,     InvoiceCustomerId AS CustomerCode,     DeliveryModeId AS DeliveryModeCode,     PaymentTermsId AS PaymentTermsCode,     DeliveryTermsId AS DeliveryTermsCode,     TransactionCurrencyId AS TransactionCurrencyCode,     LedgerCode,     OrigSalesOrderId,      -- 3‑Dates & metrics     InvoiceDate,     RequestedDeliveryDate,     ConfirmedDeliveryDate,     SalesUnit,     InvoicedQuantity,     SalesPricePerUnitTC,     GrossSalesTC,     DiscountAmountTC,     InvoicedSalesAmountTC,     MarkupAmountTC,     NetSalesTC  FROM     /* Core data – flat table derived from the staging tables and markup look‑ups. */     (         SELECT             -- Headers             CIJS.InvoiceId                                   AS SalesInvoiceId,             CASE                 WHEN LEFT(CIJS.InvoiceId, 2) = 'CI'     THEN 'Customer Invoice'                 WHEN LEFT(CIJS.InvoiceId, 3) = 'FTI'    THEN 'Customer Free Text Invoice'                 WHEN LEFT(CIJS.InvoiceId, 3) = 'SCN'    THEN 'Customer Credit Note'                 WHEN LEFT(CIJS.InvoiceId, 3) = 'FTC'    THEN 'Customer Free Text Credit Note'                 WHEN LEFT(CIJS.InvoiceId, 2) = 'RE'     THEN 'Customer Rebate'                 ELSE '_N/A'             END                                           AS TransactionType,              CITS.LineNum                                   AS SalesInvoiceLineNumber,             CONCAT(UPPER(CIJS.InvoiceId), ' - ', CAST(CITS.LineNum AS STRING))                                                     AS SalesInvoiceLineNumberCombination,              CIJS.CustInvoiceJourRecId                     AS HeaderRecId,             CITS.CustInvoiceTransRecId                    AS LineRecId,              CASE WHEN CIJS.SalesId = '' THEN NULL ELSE CIJS.SalesId END                                                     AS SalesOrderId,             CASE WHEN CITS.InventTransId = '' THEN NULL ELSE CITS.InventTransId END                                                     AS InventTransId,             CITS.InventDimId                                 AS InventDimId,              /* TAXWRITE logic – numeric prefix extraction. */             CASE                 WHEN CITS.TaxWriteCode NOT IN ('21%', '12%', '6%') THEN 0                 WHEN CITS.TaxWriteCode = '6%'                     THEN 6                 ELSE                     CASE                         WHEN LENGTH(REPLACE(CITS.TaxWriteCode, ',', '')) >= 2                         THEN CAST(SUBSTRING(REPLACE(CITS.TaxWriteCode, ',', ''), 1, 2) AS INT)                         ELSE 0                     END             END                                         AS TaxWriteCode,              'Invoiced'                                    AS SalesOrderStatus,             UPPER(CIJS.DataAreaId)                        AS CompanyId,              CASE WHEN UPPER(CITS.ItemId) = '' THEN NULL ELSE UPPER(CITS.ItemId) END                                                     AS ProductId,             CASE WHEN UPPER(CIJS.OrderAccount) = '' THEN NULL ELSE UPPER(CIJS.OrderAccount) END                                                     AS OrderCustomerId,             CASE WHEN CIJS.InvoiceAccount = '' THEN NULL ELSE CIJS.InvoiceAccount END                                                     AS InvoiceCustomerId,              CASE WHEN UPPER(CIJS.DlvMode) = '' THEN NULL ELSE UPPER(CIJS.DlvMode) END                                                     AS DeliveryModeId,             CASE WHEN UPPER(CIJS.Payment) = '' THEN NULL ELSE UPPER(CIJS.Payment) END                                                     AS PaymentTermsId,             CASE WHEN UPPER(CIJS.DlvTerm) = '' THEN NULL ELSE UPPER(CIJS.DlvTerm) END                                                     AS DeliveryTermsId,              UPPER(CITS.CurrencyCode)                     AS TransactionCurrencyId,             CITS.LEDGERDIMENSIONDISPLAYVALUE            AS LedgerCode,              CASE WHEN CITS.OrigSalesId = '' THEN NULL ELSE CITS.OrigSalesId END                                                     AS OrigSalesOrderId,              COALESCE(CAST(CIJS.InvoiceDate AS DATE), DATE('1900-01-01'))                                                     AS InvoiceDate,             DATE('1900-01-01')                           AS RequestedDeliveryDate,             COALESCE(CAST(CITS.DLVDate AS DATE), DATE('1900-01-01'))                                                     AS ConfirmedDeliveryDate,              CASE WHEN CITS.SalesUnit = '' THEN NULL ELSE CITS.SalesUnit END                                                     AS SalesUnit,             COALESCE(CITS.Qty, 0)                        AS InvoicedQuantity,             CITS.SalesPrice / CASE WHEN CITS.PriceUnit = 0 THEN 1 ELSE CITS.PriceUnit END                                                     AS SalesPricePerUnitTC,              CITS.LineAmount +             (CITS.LinePercent/100.0 * (CITS.Qty * CITS.SalesPrice / CASE WHEN CITS.PriceUnit = 0 THEN 1 ELSE CITS.PriceUnit END))             + CITS.LineDisc * CITS.Qty                 AS GrossSalesTC,              (CITS.LinePercent/100.0 * (CITS.Qty * CITS.SalesPrice / CASE WHEN CITS.PriceUnit = 0 THEN 1 ELSE CITS.PriceUnit END))             + CITS.LineDisc * CITS.Qty                 AS DiscountAmountTC,              CITS.LineAmount                                 AS InvoicedSalesAmountTC,              -- Mark‑up calculations             COALESCE(                 CASE                     WHEN COALESCE(MTS1.MarkupCategory, 0) = 0 THEN COALESCE(MTS1.Markup, 0)                     WHEN MTS1.MarkupCategory = 1 THEN COALESCE(MTS1.Markup, 0) *                                                     (CITS.SalesPrice / CASE WHEN CITS.PriceUnit = 0 THEN 1 ELSE CITS.PriceUnit END)                     WHEN MTS1.MarkupCategory = 2 THEN COALESCE(MTS1.Markup, 0) / 100.0 *                                                     (CITS.Qty * CITS.SalesPrice / CASE WHEN CITS.PriceUnit = 0 THEN 1 ELSE CITS.PriceUnit END)                 END, 0)             +             COALESCE(                 CASE                     WHEN COALESCE(MTS2.MarkupCategory, 0) = 0 THEN COALESCE(MTS2.Markup, 0)                     WHEN MTS2.MarkupCategory = 2 THEN COALESCE(MTS2.Markup, 0) / 100.0 *                                                     (CITS.Qty * CITS.SalesPrice / CASE WHEN CITS.PriceUnit = 0 THEN 1 ELSE CITS.PriceUnit END)                 END, 0)                             AS MarkupAmountTC,              -- Net sales             CITS.LineAmount -             (                 COALESCE(                     CASE                         WHEN COALESCE(MTS1.MarkupCategory, 0) = 0 THEN COALESCE(MTS1.Markup, 0)                         WHEN MTS1.MarkupCategory = 1 THEN COALESCE(MTS1.Markup, 0) *                                                     (CITS.SalesPrice / CASE WHEN CITS.PriceUnit = 0 THEN 1 ELSE CITS.PriceUnit END)                         WHEN MTS1.MarkupCategory = 2 THEN COALESCE(MTS1.Markup, 0) / 100.0 *                                                     (CITS.Qty * CITS.SalesPrice / CASE WHEN CITS.PriceUnit = 0 THEN 1 ELSE CITS.PriceUnit END)                     END, 0)                 +                 COALESCE(                     CASE                         WHEN COALESCE(MTS2.MarkupCategory, 0) = 0 THEN COALESCE(MTS2.Markup, 0)                         WHEN MTS2.MarkupCategory = 2 THEN COALESCE(MTS2.Markup, 0) / 100.0 *                                                     (CITS.Qty * CITS.SalesPrice / CASE WHEN CITS.PriceUnit = 0 THEN 1 ELSE CITS.PriceUnit END)                     END, 0)             )                                               AS NetSalesTC         FROM             `_placeholder_`.`_placeholder_`.SMRBICustInvoiceJourStaging AS CIJS         JOIN             `_placeholder_`.`_placeholder_`.SMRBICustInvoiceTransStaging AS CITS             ON CIJS.InvoiceId        = CITS.InvoiceId            AND CIJS.DataAreaId       = CITS.DataAreaId            AND CIJS.InvoiceDate      = CITS.InvoiceDate            AND CIJS.SalesId          = CITS.SalesId         LEFT JOIN             (                 SELECT                     DataAreaId,                     MarkupCategory,                     TransRecId,                     MarkupCode,                     SUM(`Value`) AS Markup                 FROM                     `_placeholder_`.`_placeholder_`.SMRBIMarkupTransStaging                 WHERE                     TransTableId IN (                         SELECT TableId                         FROM `_placeholder_`.`_placeholder_`.SqlDictionary                         WHERE TableName = 'CustInvoiceTrans'                     )                 GROUP BY                     DataAreaId,                     MarkupCategory,                     TransRecId,                     MarkupCode             ) AS MTS1             ON CITS.CustInvoiceTransRecId = MTS1.TransRecId            AND CITS.DataAreaId            = MTS1.DataAreaId         LEFT JOIN             (                 SELECT                     DataAreaId,                     MarkupCategory,                     TransRecId,                     MarkupCode,                     SUM(`Value`) AS Markup                 FROM                     `_placeholder_`.`_placeholder_`.SMRBIMarkupTransStaging                 WHERE                     TransTableId IN (                         SELECT TableId                         FROM `_placeholder_`.`_placeholder_`.SqlDictionary                         WHERE TableName = 'CustInvoiceJour'                     )                 GROUP BY                     DataAreaId,                     MarkupCategory,                     TransRecId,                     MarkupCode             ) AS MTS2             ON CIJS.CustInvoiceJourRecId = MTS2.TransRecId            AND CIJS.DataAreaId          = MTS2.DataAreaId            AND CITS.LineNum             = 1     ) AS SI;
# MAGIC --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------^^^
# MAGIC
# MAGIC ```
