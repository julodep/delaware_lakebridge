/****** Object:  View [DataStore2].[V_InventoryMovements]    Script Date: 03/03/2026 16:26:09 ******/








CREATE OR REPLACE VIEW `DataStore2`.`V_InventoryMovements` AS


SELECT
	  IT.RecId
	, ITO.ReferenceCategory AS TransType
	, ITO.InventTransId AS InventTransCode
	, IT.InventDimId AS InventDimCode
	, ITO.REFERENCEID AS ReferenceCode
	, COALESCE(NULLIF(IT.INVOICEID,''), '_N/A') AS SalesInvoiceCode
	, UPPER(IT.DataAreaId) AS CompanyCode
	, UPPER(IT.ItemId) AS ProductCode

	, COALESCE(CASE WHEN ID.WMSLocationId = '' THEN NULL 
ELSE UPPER(ID.WMSLocationId) 
END, '_N/A') AS WarehouseLocationCode

	, COALESCE(CASE WHEN ID.InventLocationId ='' THEN NULL 
ELSE UPPER(ID.InventLocationId)
END, '_N/A') AS InventLocationCode

	, COALESCE(CASE WHEN ID.InventBatchId= '' THEN NULL 
ELSE UPPER(ID.InventBatchId) 
END, '_N/A') InventBatchCode

	, IT.DatePhysical AS DatePhysical
	, IT.DateFinancial AS DateFinancial 
	, IT.DateClosed AS DateClosed
	, COALESCE(NULLIF(CAST(IT.CURRENCYCODE AS STRING),''), ITC.Currency, '_N/A') AS CurrencyCode 
	, COALESCE(CAST(NULLIF(ITD.UnitId,'') AS STRING), '_N/A') AS InventoryUnit

	, QTY = IT.QTY
	, CASE WHEN IT.StatusReceipt <> 0 OR ITC.CostPhysical IS NULL  --original: direction = 1 (receipt), but field no longer available
		   THEN IT.CostAmountPhysical 
		   ELSE ITC.CostPhysical 
	  END CostPhysicalTC

	, CASE WHEN IT.StatusReceipt <> 0 OR ITC.CostFinancial IS NULL --original: direction = 1 (receipt), but field no longer available
		   THEN CostAmountPosted+ CostAmountAdjustment
		   ELSE ITC.CostFinancial + ITC.CostAdjustment 
	  END AS CostFinancialTC

	, CASE WHEN ITC.CostFinancial IS NULL 
		   THEN 0 
		   ELSE 1 
	  END AS PriceMatch
	
FROM dbo.SMRBIInventTransStaging IT 

LEFT JOIN dbo.SMRBIInventTransOriginStaging ITO 
ON IT.InventTransOrigin = ITO.RecId

--staging table: loaded during previous step 
LEFT JOIN DataStore.InventoryMovements ITC 
ON IT.RecId = ITC.TransRecId
AND IT.DataAreaId = ITC.CompanyCode

--information about warehouse/batch
LEFT JOIN dbo.SMRBIInventDimStaging ID 
ON IT.InventDimId = ID.InventDimId
AND IT.DataAreaId = ID.DataAreaId

--Inventory unit
LEFT JOIN dbo.SMRBIInventTableModuleStaging as ITD
ON ITD.ItemId = IT.ItemId
	AND ITD.DataAreaId = IT.DataAreaId
	AND ITD.ModuleType = '0'
;
