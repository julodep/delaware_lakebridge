# Databricks notebook source
# MAGIC %md
# MAGIC # DataStore.V_PurchaseOrder.View
# MAGIC This notebook was automatically converted from the script below. It may contain errors, so use it as a starting point and make necessary corrections.
# MAGIC
# MAGIC Source script: `/Volumes/dbe_dbx_internships/datastore/datastore_volume/datastore/DataStore.V_PurchaseOrder.View.sql`

# COMMAND ----------

# -------------------------------------------------------------
# NOTE: Replace `dbe_dbx_internships` and `datastore` with the actual names
#       of your Unity Catalog catalog and schema.
# -------------------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE VIEW `dbe_dbx_internships`.`datastore`.V_PurchaseOrder AS

SELECT
    PurchaseOrderCode,
    RecId,
    PurchaseOrderLineNumber,
    OrderLineNumberCombination,
    DeliveryAddress,
    CompanyCode,
    InventTransCode,
    ProductCode,
    SupplierCode,
    OrderSupplierCode,
    DeliveryModeCode,
    PaymentTermsCode,
    DeliveryTermsCode,
    PurchaseOrderStatus,
    TransactionCurrencyCode,
    InventDimCode,
    CreationDate,
    RequestedDeliveryDate,
    ConfirmedDeliveryDate,
    OrderedQuantity,
    OrderedQuantityRemaining,
    DeliveredQuantity,
    PurchaseUnit,
    PurchasePricePerUnitTC,
    SUM(GrossPurchaseTC)          AS GrossPurchaseTC,
    SUM(DiscountAmountTC)         AS DiscountAmountTC,
    SUM(InvoicedPurchaseAmountTC) AS InvoicedPurchaseAmountTC,
    SUM(MarkupAmountTC)           AS MarkupAmountTC,
    SUM(NetPurchaseTC)            AS NetPurchaseTC

FROM
(
    SELECT
        upper(PPOHS.PurchaseOrderNumber)                            AS PurchaseOrderCode,
        PPOLS.PurchLineRecId                                       AS RecId,
        PPOLS.LineNumber                                           AS PurchaseOrderLineNumber,
        concat(
            upper(PPOHS.PurchaseOrderNumber),
            ' - ',
            cast(PPOLS.LineNumber as string)
        )                                                            AS OrderLineNumberCombination,
        coalesce(
            case when PPOHS.DeliveryAddressStreet = '' then NULL
                 else concat(
                        PPOHS.DeliveryAddressStreetNumber,
                        ' ',
                        PPOHS.DeliveryAddressStreet,
                        ', ',
                        PPOHS.DeliveryAddressZipCode,
                        ' ',
                        PPOHS.DeliveryAddressCity,
                        ', ',
                        PPOHS.DeliveryAddressCountryRegionId
                    )
            end,
            '_N/A'
        )                                                            AS DeliveryAddress,
        upper(PPOHS.DataAreaId)                                      AS CompanyCode,
        coalesce(PPOLS.InventTransId, '_N/A')                       AS InventTransCode,
        coalesce(
            case when PPOLS.ItemNumber = '' then NULL
                 else upper(PPOLS.ItemNumber)
            end,
            '_N/A'
        )                                                            AS ProductCode,
        upper(coalesce(
            case when PPOHS.InvoiceVendorAccountNumber = '' then NULL
                 else upper(PPOHS.InvoiceVendorAccountNumber)
            end,
            '_N/A'
        ))                                                           AS SupplierCode,
        upper(coalesce(
            case when PPOHS.ORDERVENDORACCOUNTNUMBER = '' then NULL
                 else upper(PPOHS.InvoiceVendorAccountNumber)
            end,
            '_N/A'
        ))                                                           AS OrderSupplierCode,
        coalesce(
            case when PPOHS.DeliveryModeId = '' then NULL
                 else upper(PPOHS.DeliveryModeId)
            end,
            '_N/A'
        )                                                            AS DeliveryModeCode,
        coalesce(
            case when PPOHS.PaymentTermsName = '' then NULL
                 else upper(PPOHS.PaymentTermsName)
            end,
            '_N/A'
        )                                                            AS PaymentTermsCode,
        coalesce(
            case when PPOHS.DeliveryTermsId = '' then NULL
                 else upper(PPOHS.DeliveryTermsId)
            end,
            '_N/A'
        )                                                            AS DeliveryTermsCode,
        coalesce(SM.Name, '_N/A')                                    AS PurchaseOrderStatus,
        upper(coalesce(PPOLS.CurrencyCode, PPOHS.CurrencyCode))      AS TransactionCurrencyCode,
        coalesce(PPOLS.InventDimId, '_N/A')                          AS InventDimCode,
        cast(PPOHS.PurchTableCreatedDateTime as date)                AS CreationDate,
        coalesce(
            coalesce(PPOLS.RequestedDeliveryDate, PPOHS.RequestedDeliveryDate),
            cast('1900-01-01' as date)
        )                                                            AS RequestedDeliveryDate,
        coalesce(PPOLS.ConfirmedDeliveryDate, cast('1900-01-01' as date)) AS ConfirmedDeliveryDate,
        coalesce(PPOLS.OrderedPurchaseQuantity, 0)                    AS OrderedQuantity,
        coalesce(VPSTS.RemainPurchPhysical, 0)                       AS OrderedQuantityRemaining,
        coalesce(PPOLS.OrderedPurchaseQuantity, 0) -
        coalesce(VPSTS.RemainPurchPhysical, 0)                       AS DeliveredQuantity,
        coalesce(NULLIF(PPOLS.PurchaseUnitSymbol, ''), 'N/A')         AS PurchaseUnit,
        coalesce(
            PPOLS.PurchasePrice / if(PPOLS.PriceUnit = 0, 1, PPOLS.PriceUnit),
            0
        )                                                            AS PurchasePricePerUnitTC,
        PPOLS.LineAmount
        + (PPOLs.LineDiscountPercentage/100.0 *
           (PPOLS.OrderedPurchaseQuantity * PPOLS.PurchasePrice / if(PPOLS.PriceQuantity = 0,1,PPOLS.PriceQuantity))
           + PPOLs.LineDiscountAmount * PPOLS.OrderedPurchaseQuantity) AS GrossPurchaseTC,
        (PPOLs.LineDiscountPercentage/100.0 *
         (PPOLS.OrderedPurchaseQuantity * PPOLS.PurchasePrice / if(PPOLS.PriceQuantity = 0,1,PPOLS.PriceQuantity))
         + PPOLs.LineDiscountAmount * PPOLS.OrderedPurchaseQuantity) AS DiscountAmountTC,
        coalesce(PPOLS.LineAmount, 0)                                AS InvoicedPurchaseAmountTC,
        (
            case
                when MC1.MarkupCategory = 0 or MC1.MarkupCategory = 2 then
                    case
                        when MC1.MarkupCategory = 0 then coalesce(MC1.Markup,0)
                        when MC1.MarkupCategory = 1 then coalesce(MC1.Markup,0) * PPOLS.PurchasePrice / if(PPOLS.PriceQuantity = 0,1,PPOLS.PriceQuantity)
                        when MC1.MarkupCategory = 2 then coalesce(MC1.Markup,0)/100.0 * (PPOLS.OrderedPurchaseQuantity * PPOLS.PurchasePrice / if(PPOLS.PriceQuantity = 0,1,PPOLS.PriceQuantity))
                    end
                else
                    case
                        when MC2.MarkupCategory = 0 then coalesce(MC2.Markup,0)
                        when MC2.MarkupCategory = 2 then coalesce(MC2.Markup,0)/100.0 * (PPOLS.OrderedPurchaseQuantity * PPOLS.PurchasePrice / if(PPOLS.PriceQuantity = 0,1,PPOLS.PriceQuantity))
                    end
            end
        ) AS MarkupAmountTC,
        PPOLS.LineAmount -
        (
            case
                when MC1.MarkupCategory = 0 then coalesce(MC1.Markup,0)
                when MC1.MarkupCategory = 1 then coalesce(MC1.Markup,0) * PPOLS.PurchasePrice / if(PPOLS.PriceQuantity = 0,1,PPOLS.PriceQuantity)
                when MC1.MarkupCategory = 2 then coalesce(MC1.Markup,0)/100.0 * (PPOLS.OrderedPurchaseQuantity * PPOLS.PurchasePrice / if(PPOLS.PriceQuantity = 0,1,PPOLS.PriceQuantity))
            end
            +
            case
                when MC2.MarkupCategory = 0 then coalesce(MC2.Markup,0)
                when MC2.MarkupCategory = 2 then coalesce(MC2.Markup,0)/100.0 * (PPOLS.OrderedPurchaseQuantity * PPOLS.PurchasePrice / if(PPOLS.PriceQuantity = 0,1,PPOSLS.PriceQuantity))
            end
        ) AS NetPurchaseTC
    FROM
        `dbe_dbx_internships`.`datastore`.SMRBIPurchPurchaseOrderHeaderStaging PPOHS
        JOIN `dbe_dbx_internships`.`datastore`.SMRBIPurchPurchaseOrderLineStaging PPOLS
            ON PPOHS.PurchaseOrderNumber = PPOLS.PurchaseOrderNumber
            AND PPOHS.DataAreaId = PPOLS.DataAreaId
        LEFT JOIN (
            SELECT
                DataAreaId,
                MarkupCategory,
                TransRecId,
                SUM(`Value`) AS Markup
            FROM `dbe_dbx_internships`.`datastore`.SMRBIMarkupTransStaging
            WHERE TransTableId IN (
                SELECT TableId FROM `dbe_dbx_internships`.`datastore`.SqlDictionary WHERE TableName = 'PurchLine'
            )
            GROUP BY DataAreaId, MarkupCategory, TransRecId
        ) MC1
            ON PPOLS.PurchLineRecId = MC1.TransRecId
            AND PPOLS.DataAreaId = MC1.DataAreaId
        LEFT JOIN (
            SELECT
                DataAreaId,
                MarkupCategory,
                TransRecId,
                SUM(`Value`) AS Markup
            FROM `dbe_dbx_internships`.`datastore`.SMRBIMarkupTransStaging
            WHERE TransTableId IN (
                SELECT TableId FROM `dbe_dbx_internships`.`datastore`.SqlDictionary WHERE TableName = 'PurchTable'
            )
            GROUP BY DataAreaId, MarkupCategory, TransRecId
        ) MC2
            ON PPOHS.PurchTableRecId = MC2.TransRecId
            AND PPOHS.DataAreaId = MC2.DataAreaId
        LEFT JOIN (
            SELECT
                VPSTS.DataAreaId,
                OrderAccount,
                OrigPurchId,
                LineNum,
                ItemId,
                InventDimId,
                SUM(Qty) AS DeliveredQty,
                MAX(VPSTS.DeliveryDate) AS MaxActualDeliveryDate
            FROM `dbe_dbx_internships`.`datastore`.SMRBIVendPackingSlipJourStaging VPSJS
            JOIN `dbe_dbx_internships`.`datastore`.SMRBIVendPackingSlipTransStaging VPSTS
                ON VPSJS.DataAreaId = VPSTS.DataAreaId
                AND VPSJS.VendPackingSlipJourRecId = VPSTS.VendPackingSlipJour
                AND VPSJS.PackingSlipId = VPSTS.PackingSlipId
            GROUP BY VPSTS.DataAreaId, VPSTS.OrigPurchId, VPSTS.OrderAccount, VPSTS.LineNum, VPSTS.ItemId, VPSTS.InventDimId
        ) VPSTS
            ON VPSTS.DataAreaId = PPOLS.DataAreaId
            AND VPSTS.OrigPurchId = PPOLS.PurchaseOrderNumber
            AND VPSTS.LineNum = PPOLS.LineNumber
            AND VPSTS.ItemId = PPOLS.ItemNumber
            AND VPSTS.InventDimId = PPOLS.InventDimId
            AND VPSTS.OrderAccount = PPOHS.OrderVendorAccountNumber
        LEFT JOIN `dbe_dbx_internships`.`datastore`.V_Date D1
            ON PPOLS.ConfirmedDeliveryDate = D1.Datetime
        LEFT JOIN `dbe_dbx_internships`.`datastore`.StringMap SM
            ON SM.SourceSystem = 'D365FO'
            AND SM.SourceTable = 'PurchPurchaseOrderLine'
            AND SM.SourceColumn = 'PurchaseOrderLineStatus'
            AND SM.Enum = coalesce(NULLIF(PPOLS.PURCHASEORDERLINESTATUS, ''), -1)
    GROUP BY
        PurchaseOrderCode,
        RecId,
        PurchaseOrderLineNumber,
        OrderLineNumberCombination,
        DeliveryAddress,
        CompanyCode,
        InventTransCode,
        ProductCode,
        SupplierCode,
        OrderSupplierCode,
        DeliveryModeCode,
        PaymentTermsCode,
        DeliveryTermsCode,
        PurchaseOrderStatus,
        TransactionCurrencyCode,
        InventDimCode,
        CreationDate,
        RequestedDeliveryDate,
        ConfirmedDeliveryDate,
        OrderedQuantity,
        OrderedQuantityRemaining,
        DeliveredQuantity,
        PurchaseUnit,
        PurchasePricePerUnitTC,
        GrossPurchaseTC,
        DiscountAmountTC,
        InvoicedPurchaseAmountTC,
        MarkupAmountTC,
        NetPurchaseTC
) PO
GROUP BY
    PurchaseOrderCode,
    RecId,
    PurchaseOrderLineNumber,
    OrderLineNumberCombination,
    DeliveryAddress,
    CompanyCode,
    InventTransCode,
    ProductCode,
    SupplierCode,
    OrderSupplierCode,
    DeliveryModeCode,
    PaymentTermsCode,
    DeliveryTermsCode,
    PurchaseOrderStatus,
    TransactionCurrencyCode,
    InventDimCode,
    CreationDate,
    RequestedDeliveryDate,
    ConfirmedDeliveryDate,
    OrderedQuantity,
    OrderedQuantityRemaining,
    DeliveredQuantity,
    PurchaseUnit,
    PurchasePricePerUnitTC;
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Static Syntax Check Results
# MAGIC No syntax errors were detected during the static check.
# MAGIC However, please review the code carefully as some issues may only be detected during runtime.
