/****** Object:  View [DataStore].[V_SalesShipment]    Script Date: 03/03/2026 16:26:08 ******/






CREATE OR REPLACE VIEW `DataStore`.`V_SalesShipment` AS


SELECT --Dimensions
	   COALESCE(CPSTS.PackingSlipId, '_N/A') AS CustPackingSlipCode
	 , COALESCE(CPSTS.LineNum, 0) AS CustPackingSlipLineNumber
	 , COALESCE(CPSTS.PackingSlipId || ' - ' || CAST(CAST(CPSTS.LineNum AS INT) AS STRING), '_N/A') AS CustPackingSlipLineNumberCombination
	 , COALESCE(CPSTS.OrigSalesId, '_N/A') AS SalesOrderCode
	 , COALESCE(CITS.InvoiceId, '_N/A') AS SalesInvoiceCode
	 , COALESCE(UPPER(CPSTS.DataAreaId), '_N/A') AS CompanyCode
	 , COALESCE(UPPER(CPSTS.ItemId), '_N/A') AS ProductCode
	 , COALESCE(CASE WHEN SOHS.OrderingCustomerAccountNumber = '' 
THEN NULL 
ELSE SOHS.OrderingCustomerAccountNumber 
END, '_N/A') AS OrderCustomerCode
	 , COALESCE(CASE WHEN SOHS.INVOICECUSTOMERACCOUNTNUMBER = '' 
THEN NULL 
ELSE SOHS.INVOICECUSTOMERACCOUNTNUMBER
END, '_N/A') AS CustomerCode
	 , COALESCE(CPSTS.InventRefTransId, '_N/A') AS InventTransCode
	 , COALESCE(CPSTS.InventDimId, '_N/A') AS InventDimCode

	   --Dates
	 , CAST(COALESCE(CPSTS.SalesLineShippingDateRequested, '1900-01-01') AS DATE) AS RequestedShippingDate
	 , CAST(COALESCE(CPSTS.SalesLineShippingDateConfirmed, '1900-01-01') AS DATE) AS ConfirmedShippingDate
	 , CAST(COALESCE(CPSTS.DeliveryDate, '1900-01-01') AS DATE) AS ActualDeliveryDate

	   --Measures
	 , COALESCE(UPPER(CPSTS.SalesUnit), '_N/A') AS SalesUnit
	 , COALESCE(CPSTS.Ordered, 0) AS OrderedQuantity
	 , COALESCE(CPSTS.Qty, 0) AS DeliveredQuantity

FROM dbo.SMRBICustPackingSlipJourStaging CPSJS

INNER JOIN 
	(
