# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_SalesOrder.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_SalesOrder.View.sql`

# COMMAND ----------

# --------------------------------------------------------------------------
# Databricks – Unity Catalog – V_SalesOrder view
# --------------------------------------------------------------------------
# The original view was written in T‑SQL for Microsoft SQL Server.
# Below is the equivalent Spark SQL (Databricks) that:
#  • uses fully‑qualified names (`dbe_dbx_internships`.`datastore`.table_or_view)
#  • replaces T‑SQL functions with their Spark equivalents
#    (ISNULL → COALESCE, CAST … AS NVARCHAR → CAST … AS STRING,
#     DATEADD → date_add, etc.)
#  • keeps the same business logic and column names
#
# NOTE: Some expressions that use square brackets (e.g. column names that
# contain spaces) are wrapped in back‑ticks.  If you encounter syntax
# errors please check the identifiers for any remaining brackets.
#
# --------------------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.V_SalesOrder AS
SELECT
    SalesOrderId                                       AS SalesOrderCode,
    SalesOrderLineNumber                               AS SalesOrderLineNumber,
    SalesOrderLineNumberCombination                    AS SalesOrderLineNumberCombination,
    OrderTransaction                                   AS OrderTransaction,
    DeliveryAddress                                    AS DeliveryAddress,
    DocumentStatus                                     AS DocumentStatus,
    HeaderRecId                                        AS HeaderRecId,
    LineRecId                                          AS LineRecId,
    DefaultDimension                                   AS DefaultDimension,
    CompanyId                                          AS CompanyCode,
    InventTransId                                      AS InventTransCode,
    InventDimId                                        AS InventDimCode,
    OrderCustomerId                                    AS OrderCustomerCode,
    InvoiceCustomerId                                  AS CustomerCode,
    ProductId                                          AS ProductCode,
    DeliveryModeId                                     AS DeliveryModeCode,
    PaymentTermsId                                     AS PaymentTermsCode,
    DeliveryTermsId                                    AS DeliveryTermsCode,
    SalesOrderStatus                                   AS SalesOrderStatus,
    TransactionCurrencyId                              AS TransactionCurrencyCode,
    CreationDate                                       AS CreationDate,
    RequestedShippingDate                              AS RequestedShippingDate,
    ConfirmedShippingDate                              AS ConfirmedShippingDate,
    RequestedDeliveryDate                              AS RequestedDeliveryDate,
    ConfirmedDeliveryDate                              AS ConfirmedDeliveryDate,
    FirstShipmentDate                                  AS FirstShipmentDate,
    LastShipmentDate                                   AS LastShipmentDate,
    SalesUnit                                          AS SalesUnit,
    OrderedQuantity                                    AS OrderedQuantity,
    OrderedQuantityRemaining                           AS OrderedQuantityRemaining,
    DeliveredQuantity                                  AS DeliveredQuantity,
    SalesPricePerUnitTC                                AS SalesPricePerUnitTC,
    GrossSalesTC                                       AS GrossSalesTC,
    DiscountAmountTC                                   AS DiscountAmountTC,
    InvoicedSalesAmountTC                              AS InvoicedSalesAmountTC,
    MarkupAmountTC                                     AS MarkupAmountTC,
    NetSalesTC                                         AS NetSalesTC
FROM (
    SELECT
        coalesce(sohs.SalesOrderNumber, '_N/A')                                          AS SalesOrderId,
        solh.LineNum                                                                  AS SalesOrderLineNumber,
        concat(sohs.SalesOrderNumber, ' - ', cast(solh.LineNum as string))           AS SalesOrderLineNumberCombination,
        CASE WHEN NULLIF(sohs.ReturnItemNum, '') IS NOT NULL THEN 'Return Order'
             ELSE 'Sales Order' END                                                   AS OrderTransaction,
        coalesce(
            case
                when sohs.DeliveryAddressStreet = '' then null
                else concat(
                        sohs.DeliveryAddressStreetNumber, ' ',
                        sohs.DeliveryAddressStreet, ', ',
                        sohs.DeliveryAddressZipCode, ' ',
                        sohs.DeliveryAddressCity, ', ',
                        sohs.DeliveryAddressCountryRegionId
                    )
            end,
            '_N/A')                                                                   AS DeliveryAddress,
        coalesce(strm.Name, '_N/A')                                                    AS DocumentStatus,
        sohs.SalesTableRecId                                                          AS HeaderRecId,
        solh.SalesLineRecId                                                            AS LineRecId,
        solh.DefaultDimension                                                          AS DefaultDimension,
        upper(sohs.DataAreaId)                                                         AS CompanyId,
        coalesce(solh.InventoryLotId, '_N/A')                                          AS InventTransId,
        coalesce(solh.InventDimId, '_N/A')                                            AS InventDimId,
        coalesce(
            case when sohs.InvoiceCustomerAccountNumber = '' then null
                 else upper(sohs.InvoiceCustomerAccountNumber) end,
            '_N/A')                                                                     AS InvoiceCustomerId,
        coalesce(
            case when sohs.OrderingCustomerAccountNumber = '' then null
                 else upper(sohs.OrderingCustomerAccountNumber) end,
            '_N/A')                                                                     AS OrderCustomerId,
        coalesce(
            case when solh.ItemNumber = '' then null
                 else upper(solh.ItemNumber) end,
            '_N/A')                                                                     AS ProductId,
        coalesce(
            case when sohs.DeliveryModeCode = '' then null
                 else upper(sohs.DeliveryModeCode) end,
            '_N/A')                                                                     AS DeliveryModeId,
        coalesce(
            case when sohs.PaymentTermsName = '' then null
                 else upper(sohs.PaymentTermsName) end,
            '_N/A')                                                                     AS PaymentTermsId,
        coalesce(
            case when sohs.DeliveryTermsCode = '' then null
                 else upper(sohs.DeliveryTermsCode) end,
            '_N/A')                                                                     AS DeliveryTermsId,
        coalesce(sm.Name, 'Unknown')                                                   AS SalesOrderStatus,
        upper(sohs.CurrencyCode)                                                      AS TransactionCurrencyId,
        cast(coalesce(sohs.SalesTableCreatedDateTime, '1900-01-01') as date)          AS CreationDate,
        cast(coalesce(coalesce(sohs.RequestedShippingDate, solh.RequestedShippingDate), '1900-01-01') as date)
                                                                                     AS RequestedShippingDate,
        cast(coalesce(coalesce(solh.ConfirmedShippingDate, sohs.ConfirmedShippingDate), '1900-01-01') as date)
                                                                                     AS ConfirmedShippingDate,
        cast(coalesce(coalesce(sohs.RequestedReceiptDate, solh.RequestedReceiptDate), '1900-01-01') as date)
                                                                                     AS RequestedDeliveryDate,
        cast(coalesce(coalesce(solh.ConfirmedReceiptDate, sohs.ConfirmedReceiptDate), '1900-01-01') as date)
                                                                                     AS ConfirmedDeliveryDate,
        cast(coalesce(cpst.FirstShipmentDate, '1900-01-01') as date)                  AS FirstShipmentDate,
        cast(coalesce(cpst.LastShipmentDate, '1900-01-01') as date)                   AS LastShipmentDate,
        coalesce(upper(solh.SalesUnitSymbol), '_N/A')                                 AS SalesUnit,
        coalesce(solh.OrderedSalesQuantity, 0)                                        AS OrderedQuantity,
        coalesce(solh.RemainSalesPhysical, 0)                                       AS OrderedQuantityRemaining,
        coalesce(solh.OrderedSalesQuantity, 0) - coalesce(solh.RemainSalesPhysical, 0) AS DeliveredQuantity,
        solh.SalesPrice / CASE WHEN solh.SalesPriceQuantity = 0 THEN 1
                               ELSE solh.SalesPriceQuantity END                     AS SalesPricePerUnitTC,
        solh.LineAmount +
            (solh.LineDiscountPercentage/100.0 *
                (solh.OrderedSalesQuantity * solh.SalesPrice /
                     CASE WHEN solh.SalesPriceQuantity = 0 THEN 1
                          ELSE solh.SalesPriceQuantity END))
            + solh.LineDiscountAmount * solh.OrderedSalesQuantity                   AS GrossSalesTC,
        solh.LineDiscountPercentage/100.0 *
            (solh.OrderedSalesQuantity * solh.SalesPrice /
                 CASE WHEN solh.SalesPriceQuantity = 0 THEN 1
                      ELSE solh.SalesPriceQuantity END)
            + solh.LineDiscountAmount * solh.OrderedSalesQuantity                   AS DiscountAmountTC,
        solh.LineAmount                                                              AS InvoicedSalesAmountTC,
        (CASE WHEN coalesce(mdce1.MarkupCategory,0) = 0 THEN coalesce(mdce1.Markup,0)
              WHEN mdce1.MarkupCategory = 1 THEN coalesce(mdce1.Markup,0) *
                     solh.SalesPrice / CASE WHEN solh.SalesPriceQuantity = 0 THEN 1
                                           ELSE solh.SalesPriceQuantity END
              WHEN mdce1.MarkupCategory = 2 THEN coalesce(mdce1.Markup,0)/100.0 *
                     (solh.OrderedSalesQuantity * solh.SalesPrice /
                       CASE WHEN solh.SalesPriceQuantity = 0 THEN 1
                            ELSE solh.SalesPriceQuantity END)
         END
          + CASE WHEN coalesce(mdce2.MarkupCategory,0) = 0 THEN coalesce(mdce2.Markup,0)
                 WHEN mdce2.MarkupCategory = 2 THEN coalesce(mdce2.Markup,0)/100.0 *
                     (solh.OrderedSalesQuantity * solh.SalesPrice /
                       CASE WHEN solh.SalesPriceQuantity = 0 THEN 1
                            ELSE solh.SalesPriceQuantity END)
            END
         ) AS MarkupAmountTC,
        solh.LineAmount -
            (CASE WHEN coalesce(mdce1.MarkupCategory,0) = 0 THEN coalesce(mdce1.Markup,0)
                  WHEN mdce1.MarkupCategory = 1 THEN coalesce(mdce1.Markup,0) *
                         solh.SalesPrice / CASE WHEN solh.SalesPriceQuantity = 0 THEN 1
                                               ELSE solh.SalesPriceQuantity END
                  WHEN mdce1.MarkupCategory = 2 THEN coalesce(mdce1.Markup,0)/100.0 *
                         (solh.OrderedSalesQuantity * solh.SalesPrice /
                           CASE WHEN solh.SalesPriceQuantity = 0 THEN 1
                                ELSE solh.SalesPriceQuantity END)
             END
             + CASE WHEN coalesce(mdce2.MarkupCategory,0) = 0 THEN coalesce(mdce2.Markup,0)
                    WHEN mdce2.MarkupCategory = 2 THEN coalesce(mdce2.Markup,0)/100.0 *
                         (solh.OrderedSalesQuantity * solh.SalesPrice /
                           CASE WHEN solh.SalesPriceQuantity = 0 THEN 1
                                ELSE solh.SalesPriceQuantity END)
               END
            ) AS NetSalesTC
    FROM dbe_dbx_internships.dbo.SMRBISalesOrderHeaderStaging SOHS
    JOIN dbe_dbx_internships.dbo.SMRBISalesOrderLineStaging SOLH
        ON SOHS.SalesOrderNumber = SOLH.SalesOrderNumber
       AND SOHS.DataAreaId = SOLH.DataAreaId
    LEFT JOIN (
        SELECT DataAreaId, MarkupCategory, TransRecId,
               SUM(Value) AS Markup
        FROM dbe_dbx_internships.dbo.SMRBIMarkupTransStaging
        WHERE TransTableId IN (
              SELECT TableId
              FROM dbe_dbx_internships.etl.SqlDictionary
              WHERE TableName = 'SalesLine'
        )
        GROUP BY DataAreaId, MarkupCategory, TransRecId
    ) MDCE1
        ON MDCE1.TransRecId = SOLH.SalesLineRecId
       AND MDCE1.DataAreaId = SOLH.DataAreaId
    LEFT JOIN (
        SELECT DataAreaId, MarkupCategory, TransRecId,
               SUM(Value) AS Markup
        FROM dbe_dbx_internships.dbo.SMRBIMarkupTransStaging
        WHERE TransTableId IN (
              SELECT TableId
              FROM dbe_dbx_internships.etl.SqlDictionary
              WHERE TableName = 'SalesTable'
        )
        GROUP BY DataAreaId, MarkupCategory, TransRecId
    ) MDCE2
        ON MDCE2.TransRecId = SOLH.SalesLineRecId
       AND MDCE2.DataAreaId = SOLH.DataAreaId
       AND SOLH.LineNum = 1
    LEFT JOIN (
        SELECT DataAreaId, ItemId, SalesId, InventTransId, InventDimId,
               MIN(DeliveryDate) AS FirstShipmentDate,
               MAX(DeliveryDate) AS LastShipmentDate,
               SUM(Ordered)     AS Ordered,
               SUM(Qty)         AS Qty
        FROM dbe_dbx_internships.dbo.SMRBICustPackingSlipTransStaging
        GROUP BY DataAreaId, ItemId, SalesId, InventTransId, InventDimId
    ) CPST
        ON CPST.DataAreaId = SOLH.DataAreaId
       AND CPST.InventDimId = SOLH.InventDimId
       AND CPST.SalesId = SOLH.SalesOrderNumber
       AND CPST.ItemId = SOLH.ItemNumber
       AND CPST.InventTransId = SOLH.InventoryLotId
    LEFT JOIN dbe_dbx_internships.etl.StringMap STRM
        ON STRM.SourceTable = 'SalesOrder'
       AND STRM.SourceColumn = 'DocumentStatus'
       AND SOHS.DocumentStatus = STRM.Enum
    LEFT JOIN dbe_dbx_internships.etl.StringMap SM
        ON SM.SourceSystem = 'D365FO'
       AND SM.SourceTable = 'SalesOrderLine'
       AND SM.SourceColumn = 'SalesOrderLineStatus'
       AND SM.Enum = coalesce(NULLIF(SOLH.SalesOrderLineStatus, ''), 0)
) SO
GROUP BY
    SalesOrderId,
    SalesOrderLineNumber,
    SalesOrderLineNumberCombination,
    OrderTransaction,
    DeliveryAddress,
    DocumentStatus,
    HeaderRecId,
    LineRecId,
    DefaultDimension,
    CompanyId,
    InventTransId,
    InventDimId,
    OrderCustomerId,
    InvoiceCustomerId,
    ProductId,
    DeliveryModeId,
    PaymentTermsId,
    DeliveryTermsId,
    SalesOrderStatus,
    TransactionCurrencyId,
    CreationDate,
    RequestedShippingDate,
    ConfirmedShippingDate,
    RequestedDeliveryDate,
    ConfirmedDeliveryDate,
    FirstShipmentDate,
    LastShipmentDate,
    SalesUnit,
    OrderedQuantity,
    OrderedQuantityRemaining,
    DeliveredQuantity,
    SalesPricePerUnitTC;
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
