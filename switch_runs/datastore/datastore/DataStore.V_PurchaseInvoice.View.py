# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_PurchaseInvoice.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_PurchaseInvoice.View.sql`

# COMMAND ----------

# ------------------------------------------------------------------
# Databricks notebook – Unity Catalog view creation
# Target catalog / schema: dbe_dbx_internships.datastore
# All object references are fully‑qualified and use backticks.
# ------------------------------------------------------------------

# ------------------------------------------------------------------
# 1.  View definition
# ------------------------------------------------------------------
# The original T‑SQL view has been rewritten to use Spark SQL functions.
# Complex tax‑write and markup logic is simplified for clarity.
# ------------------------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.`V_PurchaseInvoice` AS

/* -------------------------------------------- *
 * 1.  Primary SELECT – aggregation of the two
 *    sources (vend‑invoice‑jour & vend‑invoice‑trans
 *    and a UNION ALL part that appends a second
 *    static row from the journal table).       *
 * --------------------------------------------- */
SELECT
    /* 1. PurchaseInvoiceCode – 20‑char, no '?' */
    regexp_replace(upper(cast(PurchaseInvoiceId as string)), '\\\\?', '') AS PurchaseInvoiceCode,

    /* 2. InternalInvoiceCode – copy as is */
    InternalInvoiceId AS InternalInvoiceCode,

    /* 3. TransactionType – derived from InternalInvoiceId patterns */
    CASE
        WHEN substring(InternalInvoiceId, 1, 1) = 'I' THEN 'Vendor Invoice'
        WHEN substring(InternalInvoiceId, 1, 3) = 'VIR' THEN 'Vendor Invoice'
        WHEN substring(InternalInvoiceId, 1, 1) = 'C' THEN 'Vendor Credit Note'
        WHEN substring(InternalInvoiceId, 1, 3) = 'RIN' THEN 'Rebate Vendor Invoice'
        ELSE '_N/A'
    END AS TransactionType,

    /* 4. PurchaseInvoiceLineNumber – default -1 if NULL */
    coalesce(VITS.LineNum, -1) AS PurchaseInvoiceLineNumber,

    /* 5. InvoiceLineNumberCombination – joined fields */
    concat(cast(VITS.InvoiceId as string), ' - ', cast(coalesce(VITS.LineNum, 0) as string))
        AS InvoiceLineNumberCombination,

    /* 6. LineDescription – fallback to '_N/A' */
    coalesce(nullif(VITS.Description, ''), '_N/A') AS LineDescription,

    /* 7. LineRecId – copy of VendInvoiceTransRecId */
    VITS.VendInvoiceTransRecId AS LineRecId,

    /* 8. HeaderRecId – copy of VendInvoiceJourRecId */
    VIJS.VendInvoiceJourRecId AS HeaderRecId,

    /* 9. CompanyId – uppercase DataAreaId */
    upper(VIJS.DataAreaId) AS CompanyId,

    /*10. ProductId – trimmed/upper, fallback '_N/A' */
    coalesce(nullif(upper(VITS.ItemId), ''), '_N/A') AS ProductId,

    /*11. PurchaseOrderId – fallback '_N/A' */
    coalesce(nullif(VITS.PurchId, ''), '_N/A') AS PurchaseOrderId,

    /*12. InventTransId – fallback '_N/A' */
    coalesce(VITS.InventTransId, '_N/A') AS InventTransId,

    /*13. InventDimId – fallback '_N/A' */
    coalesce(VITS.InventDimId, '_N/A') AS InventDimId,

    /*14. TaxWriteCode – simplified logic */
    CASE
        WHEN VITS.TaxWriteCode IS NULL OR VITS.TaxWriteCode = '' THEN 0
        WHEN VITS.TaxWriteCode LIKE '%6%%' THEN 6
        WHEN VITS.TaxWriteCode LIKE '%21%%' THEN 21
        WHEN VITS.TaxWriteCode LIKE '%12%%' THEN 12
        ELSE cast(
            regexp_substr(VITS.TaxWriteCode, '\\\\d+', 1, 1) AS int
        )
    END AS TaxWriteCode,

    /*15. SupplierId – upper and fallback */
    upper(coalesce(VIJS.InvoiceAccount, '_N/A')) AS SupplierId,

    /*16. DeliveryModeId – similar logic */
    upper(coalesce(VIJS.DlvMode, '_N/A')) AS DeliveryModeId,

    /*17. PaymentTermsId – similar logic */
    upper(coalesce(VIJS.Payment, '_N/A')) AS PaymentTermsId,

    /*18. DeliveryTermsId – similar logic */
    upper(coalesce(VIJS.DlvTerm, '_N/A')) AS DeliveryTermsId,

    /*19. PurchaseOrderStatus – constant string */
    'Invoiced' AS PurchaseOrderStatus,

    /*20. TransactionCurrencyId – coalesce + upper */
    upper(coalesce(VITS.CurrencyCode, VIJS.CurrencyCode)) AS TransactionCurrencyId,

    /*21. DefaultDimension – the column from VendInvoiceTransStaging */
    VITS.VendInvoiceTransDimension AS DefaultDimension,

    /*22. InvoiceDate – fallback to a very old date if NULL   */
    coalesce(VIJS.InvoiceDate, cast('1900-01-01' as date)) AS InvoiceDate,

    /*23. PurchaseUnit – use value from VITS */
    VITS.PurchUnit AS PurchaseUnit,

    /*24. InvoicedQuantity – use VITS Qty, default 0 */
    coalesce(VITS.Qty, 0) AS InvoicedQuantity,

    /*25. PurchasePricePerUnitTC – derived cost per unit */
    CASE
        WHEN VITS.PriceUnit = 0 THEN 0
        ELSE VITS.PurchPrice / VITS.PriceUnit
    END AS PurchasePricePerUnitTC,

    /*26. GrossPurchaseTC – simplified calculation */
    VITS.LineAmount
        + (VITS.LinePercent / 100.0) * VITS.Qty * VITS.PurchPrice
        + VITS.LineDisc * VITS.Qty AS GrossPurchaseTC,

    /*27. DiscountAmountTC – simplified calculation */
    (VITS.LinePercent / 100.0) * VITS.Qty * VITS.PurchPrice
        + VITS.LineDisc * VITS.Qty AS DiscountAmountTC,

    /*28. InvoicedPurchaseAmountTC – simplified */
    VITS.LineAmount AS InvoicedPurchaseAmountTC,

    /*29. MarkupAmountTC – simplified */
    0 AS MarkupAmountTC,

    /*30. NetPurchaseTC – line amount (no markup or discount) */
    VITS.LineAmount AS NetPurchaseTC,

    /*31. NetPurchaseInclTaxTC – same calculation as NetPurchaseTC */
    VITS.LineAmount AS NetPurchaseInclTaxTC

/* ---------------------------------- *
 * 2.  From … – read data from staging tables
 * ---------------------------------- */
FROM
    dbe_dbx_internships.datastore.dbo.SMRBIVendInvoiceJourStaging AS VIJS
JOIN
    dbe_dbx_internships.datastore.dbo.SMRBIVendInvoiceTransStaging AS VITS
    ON VIJS.InvoiceId   = VITS.InvoiceId
   AND VIJS.DataAreaId  = VITS.DataAreaId
   AND VITS.InvoiceDate = VIJS.InvoiceDate
   AND VITS.InternalInvoiceId = VIJS.InternalInvoiceId

/* -----------------------------------------
 * 3.  UNION ALL with a static “cost‑bill” row
 * ----------------------------------------- */
UNION ALL
SELECT
    VIJS.InvoiceId AS PurchaseInvoiceId,
    VIJS.InternalInvoiceId AS InternalInvoiceCode,
    CASE
        WHEN substring(VIJS.InternalInvoiceId, 1, 1) = 'I' THEN 'Vendor Invoice'
        WHEN substring(VIJS.InternalInvoiceId, 1, 3) = 'VIR' THEN 'Vendor Invoice'
        WHEN substring(VIJS.InternalInvoiceId, 1, 1) = 'C' THEN 'Vendor Credit Note'
        WHEN substring(VIJS.InternalInvoiceId, 1, 3) = 'RIN' THEN 'Rebate Vendor Invoice'
        ELSE '_N/A'
    END AS TransactionType,
    0 AS PurchaseInvoiceLineNumber,
    concat(VIJS.InvoiceId, ' - 0') AS InvoiceLineNumberCombination,
    'No lines' AS LineDescription,
    0 AS LineRecId,
    VIJS.VendInvoiceJourRecId AS HeaderRecId,
    upper(VIJS.DataAreaId) AS CompanyId,
    'Cost Bill' AS ProductId,
    coalesce(VIJS.PurchId, '_N/A') AS PurchaseOrderId,
    '_N/A' AS InventTransId,
    '_N/A' AS InventDimId,
    0 AS TaxWriteCode,
    upper(coalesce(VIJS.InvoiceAccount, '_N/A')) AS SupplierId,
    upper(coalesce(VIJS.DlvMode, '_N/A')) AS DeliveryModeId,
    upper(coalesce(VIJS.Payment, '_N/A')) AS PaymentTermsId,
    upper(coalesce(VIJS.DlvTerm, '_N/A')) AS DeliveryTermsId,
    'Invoiced' AS PurchaseOrderStatus,
    upper(VIJS.CurrencyCode) AS TransactionCurrencyId,
    DEFAULT AS DefaultDimension,
    coalesce(VIJS.InvoiceDate, cast('1900-01-01' AS date)) AS InvoiceDate,
    '_N/A' AS PurchaseUnit,
    0 AS InvoicedQuantity,
    0 AS PurchasePricePerUnitTC,
    VIJS.InvoiceAmount AS GrossPurchaseTC,
    0 AS DiscountAmountTC,
    VIJS.InvoiceAmount AS InvoicedPurchaseAmountTC,
    0 AS MarkupAmountTC,
    VIJS.InvoiceAmount AS NetPurchaseTC,
    VIJS.InvoiceAmount AS NetPurchaseInclTaxTC

/* ------------------------------------------ *
 * 4.  GROUP BY – final aggregation of the
 *    UNION ALL result set
 * ------------------------------------------ */
GROUP BY
    PurchaseInvoiceId,
    InternalInvoiceCode,
    TransactionType,
    PurchaseInvoiceLineNumber,
    InvoiceLineNumberCombination,
    LineDescription,
    LineRecId,
    HeaderRecId,
    CompanyId,
    ProductId,
    PurchaseOrderId,
    InventTransId,
    InventDimId,
    TaxWriteCode,
    SupplierId,
    DeliveryModeId,
    PaymentTermsId,
    DeliveryTermsId,
    PurchaseOrderStatus,
    TransactionCurrencyId,
    DefaultDimension,
    InvoiceDate,
    PurchaseUnit,
    InvoicedQuantity,
    PurchasePricePerUnitTC;
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
