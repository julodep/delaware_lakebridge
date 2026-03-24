/****** Object:  View [DataStore2].[V_NetRequirements]    Script Date: 03/03/2026 16:26:08 ******/










CREATE OR REPLACE VIEW `DataStore2`.`V_NetRequirements` AS


SELECT    NR.RecId
		, NR.CompanyCode AS CompanyCode
		, NR.ReferenceType
		, NR.PlanVersion
		, NR.ProductCode AS ProductCode
		, NR.InventDimCode
		, NR.RequirementDate
		, NR.RequirementTime
		, NR.RequirementDateTime
		, NR.ReferenceCode
		, NR.ProducedItemCode
		, NR.CustomerCode
		, COALESCE(NULLIF(NR.VendorCode,'_N/A'),NULLIF(VEN.VendorAccountNumber,''),'_N/A') AS VendorCode
		, NR.ActionDate
		, NR.ActionDays
		, NR.ActionType
		, NR.ActionMarked
		, NR.FuturesDate
		, NR.FuturesDays
		, NR.FuturesCalculated
		, NR.FuturesMarked
		, NR.Direction
	/* Determine OOS */
		, NR.RankNr AS RankNr
		, CASE 
				WHEN NR.AccumulatedQuantity >= 0 THEN 0
				WHEN NR.AccumulatedQuantity < 0 
					THEN 1
				WHEN PQ.AccumulatedQuantity >= 0 AND NR.AccumulatedQuantity < 0 THEN 1
				ELSE 0 
		   END AS FictionalOOS
		, CASE 
				WHEN NR.AccumulatedQuantity >= 0 THEN 0
				WHEN PQA.QuantityOfTheDay < 0 AND NR.RankNr = 1 THEN 1
				WHEN PQ.AccumulatedQuantity >= 0 AND PQ.AccumulatedQuantity + PQA.QuantityOfTheDay < 0 THEN 1
				ELSE 0 
		   END AS ActualOOS
		, CASE 
				WHEN NR.AccumulatedQuantityConfirmed >= 0 THEN 0
				WHEN NR.AccumulatedQuantityConfirmed < 0
					THEN 1
				WHEN PQ.AccumulatedQuantityConfirmed >= 0 AND NR.AccumulatedQuantityConfirmed < 0 THEN 1
				ELSE 0 
		  END AS FictionalOOS_Confirmed
		, CASE 
				WHEN NR.AccumulatedQuantityConfirmed >= 0 THEN 0
				WHEN PQA.QuantityOfTheDayConfirmed < 0 AND NR.RankNr = 1 THEN 1 
				WHEN PQ.AccumulatedQuantityConfirmed >= 0 AND PQ.AccumulatedQuantityConfirmed + PQA.QuantityOfTheDayConfirmed < 0 THEN 1
				ELSE 0 END 
		  AS ActualOOS_Confirmed 
	/* Quantities */
		, COALESCE(NR.Quantity, 0) AS Quantity_InventoryUnit
		, COALESCE(CASE WHEN P.ProductInventoryUnit = P.ProductPurchaseUnit
THEN NR.Quantity 
ELSE NR.Quantity * UOM1.Factor 
END, 0) AS Quantity_PurchaseUnit
		, COALESCE(CASE WHEN P.ProductInventoryUnit = P.ProductSalesUnit
THEN NR.Quantity 
ELSE NR.Quantity * UOM2.Factor 
END, 0) AS Quantity_SalesUnit
		
		, COALESCE(NR.AccumulatedQuantity, 0) AS AccumulatedQuantity_InventoryUnit
		, COALESCE(CASE WHEN P.ProductInventoryUnit = P.ProductPurchaseUnit
THEN NR.AccumulatedQuantity 
ELSE NR.AccumulatedQuantity * UOM1.Factor 
END, 0) AS AccumulatedQuantity_PurchaseUnit
		, COALESCE(CASE WHEN P.ProductInventoryUnit = P.ProductSalesUnit
THEN NR.AccumulatedQuantity 
ELSE NR.AccumulatedQuantity * UOM2.Factor 
END, 0) AS AccumulatedQuantity_SalesUnit
		
		-- Confirmed quantities (no planned and forecasted quantities)

		, COALESCE(NR.QuantityConfirmed, 0) AS QuantityConfirmed_InventoryUnit
		, COALESCE(CASE WHEN P.ProductInventoryUnit = P.ProductPurchaseUnit
THEN NR.QuantityConfirmed 
ELSE NR.QuantityConfirmed * UOM1.Factor 
END, 0) AS QuantityConfirmed_PurchaseUnit
		, COALESCE(CASE WHEN P.ProductInventoryUnit = P.ProductSalesUnit
THEN NR.QuantityConfirmed 
ELSE NR.QuantityConfirmed * UOM2.Factor 
END, 0) AS QuantityConfirmed_SalesUnit
		
		, COALESCE(NR.AccumulatedQuantityConfirmed, 0) AS AccumulatedQuantityConfirmed_InventoryUnit
		, COALESCE(CASE WHEN P.ProductInventoryUnit = P.ProductPurchaseUnit
THEN NR.AccumulatedQuantityConfirmed 
ELSE NR.AccumulatedQuantityConfirmed * UOM1.Factor 
END, 0) AS AccumulatedQuantityConfirmed_PurchaseUnit
		, COALESCE(CASE WHEN P.ProductInventoryUnit = P.ProductSalesUnit
THEN NR.AccumulatedQuantityConfirmed 
ELSE NR.AccumulatedQuantityConfirmed * UOM2.Factor 
END, 0) AS AccumulatedQuantityConfirmed_SalesUnit
		 
		, COALESCE(CP1.Price * NR.Quantity, 0) AS ValueAC
		, COALESCE(CASE WHEN L.GroupCurrency = L.AccountingCurrency THEN CP1.Price ELSE CP1.Price * GC_CP.ExchangeRate END * NR.Quantity, 0) AS ValueGC
		, COALESCE(CP1.Price, 0) AS PriceAC
		, COALESCE(CP1.Price * GC_CP.ExchangeRate, 0) AS PriceGC
		
FROM 

	(SELECT	  RecId
			, CompanyCode
			, ReferenceType
			, PlanVersion
			, ProductCode
			, InventDimCode
			, RequirementDate
			, RequirementTime
			, RequirementDateTime
			, ReferenceCode
			, ProducedItemCode
			, CustomerCode
			, VendorCode
			, ActionDate
			, ActionDays
			, ActionType
			, ActionMarked
			, FuturesDate
			, FuturesDays
			, FuturesCalculated
			, FuturesMarked
			, Direction
			, RankNr
			, Quantity
			, QuantityConfirmed
			, AccumulatedQuantity = CASE WHEN RankNr = 0 THEN 0 ELSE SUM(Quantity) OVER (PARTITION BY CompanyCode, PlanVersion, ProductCode, InventDimCode ORDER BY RankNr ASC) END
			, AccumulatedQuantityConfirmed = CASE WHEN RankNr = 0 THEN 0 ELSE SUM(QuantityConfirmed) OVER (PARTITION BY CompanyCode, PlanVersion, ProductCode, InventDimCode ORDER BY RankNr ASC) END
	FROM 
		(SELECT *
			FROM DataStore.NetRequirements
			WHERE 1=1
				and RankNr != 0) NR

	UNION ALL 

	SELECT	  RecId
			, CompanyCode
			, ReferenceType
			, PlanVersion
			, ProductCode
			, InventDimCode
			, RequirementDate
			, RequirementTime
			, RequirementDateTime
			, ReferenceCode
			, ProducedItemCode
			, CustomerCode
			, VendorCode
			, ActionDate
			, ActionDays
			, ActionType
			, ActionMarked
			, FuturesDate
			, FuturesDays
			, FuturesCalculated
			, FuturesMarked
			, Direction
			, RankNr
			, Quantity
			, QuantityConfirmed
			, Quantity AS AccumulatedQuantity
			, QuantityConfirmed AS AccumulatedQuantityConfirmed

	FROM 
		(SELECT *
			FROM DataStore.NetRequirements
			WHERE 1=1
				and RankNr = 0) NR) NR

JOIN DataStore.Product P
ON NR.CompanyCode = P.CompanyCode
	and NR.ProductCode = P.ProductCode

--Self join for flag OutOfStock (OOS), only 1 if Rank-1 > 0

LEFT JOIN 
	(SELECT	RecId
			, CompanyCode
			, ReferenceType
			, PlanVersion
			, ProductCode
			, InventDimCode
			, RankNr
			, Quantity
			, CASE WHEN RankNr = 0 
				   THEN 0 
				   ELSE SUM(Quantity) OVER (PARTITION BY CompanyCode, PlanVersion, ProductCode, InventDimCode ORDER BY RankNr ASC) 
			  END AS AccumulatedQuantity
			, CASE WHEN RankNr = 0 
				   THEN 0 
				   ELSE SUM(QuantityConfirmed) OVER (PARTITION BY CompanyCode, PlanVersion, ProductCode, InventDimCode ORDER BY RankNr ASC) 
			  END AccumulatedQuantityConfirmed
	FROM 
		(SELECT *
			FROM DataStore.NetRequirements
			WHERE 1=1
				and RankNr != 0) NR2 ) PQ
ON NR.CompanyCode = PQ.CompanyCode
	AND NR.PlanVersion = PQ.PlanVersion
	AND NR.ProductCode = PQ.ProductCode
	AND NR.InventDimCode = PQ.InventDimCode
	AND NR.RankNr = PQ.RankNr+1

-- Self join for flag Actual_OOS, calculate the total used+purchased quantity of that day

LEFT JOIN 
	(SELECT	 CompanyCode
			, PlanVersion
			, RequirementDate
			, ProductCode
			, InventDimCode
			, SUM(Quantity) AS QuantityOfTheDay
			, SUM(QuantityConfirmed) AS QuantityOfTheDayConfirmed
	FROM 
		(SELECT *
			FROM DataStore.NetRequirements
			WHERE 1=1
				and RankNr != 0
				and RequirementDate <> '1900-01-01') NR2
				
	GROUP BY CompanyCode, PlanVersion, ProductCode, InventDimCode, RequirementDate
	
			 ) PQA
ON NR.CompanyCode = PQA.CompanyCode
	AND NR.PlanVersion = PQA.PlanVersion
	AND NR.ProductCode = PQA.ProductCode
	AND NR.InventDimCode = PQA.InventDimCode
	AnD NR.RequirementDate = PQA.RequirementDate


--Required for Unit of Measure:

LEFT JOIN DataStore.UnitOfMeasure UOM1
ON NR.ProductCode = UOM1.ItemNumber
	and NR.CompanyCode = UOM1.CompanyCode
	and UOM1.FromUOM = P.ProductInventoryUnit
	and UOM1.ToUOM = P.ProductPurchaseUnit
LEFT JOIN DataStore.UnitOfMeasure UOM2
ON NR.ProductCode = UOM2.ItemNumber
	and NR.CompanyCode = UOM2.CompanyCode
	and UOM2.FromUOM = P.ProductInventoryUnit
	and UOM2.ToUOM = P.ProductSalesUnit

--Step 1: Join on Product Configuration to retrieve the correct items configurations for the cost prices (see next join) and To join to ReqItemCoverageStaging (vendor per product)
LEFT JOIN DataStore.ProductConfiguration PC1 
ON NR.CompanyCode = PC1.CompanyCode
	AND NR.InventDimCode = PC1.InventDimCode
	
--To get vendors for Raw Materials, Utilities and packing
LEFT JOIN 
	( SELECT 
		DataAreaId, CoverageWarehouseId, CoverageProductConfigurationId, ItemNumber, VendorAccountNumber
		FROM dbo.SMRBIReqItemCoverageSettingsStaging 
		GROUP BY DataAreaId, CoverageWarehouseId, CoverageProductConfigurationId, ItemNumber, VendorAccountNumber) VEN
ON NR.CompanyCode = UPPER(VEN.DataAreaId)
	AND PC1.ProductConfigurationCode = VEN.CoverageProductConfigurationId
	AND UPPER(PC1.WarehouseCode) = UPPER(VEN.CoverageWarehouseId)
	AND NR.ProductCode = VEN.ItemNumber
	AND LEFT(VEN.ItemNumber,1) <> '5'


--Step 2a: Join directly on DataStore.CostPrice if a cost price exists for the inventory unit of the BOM lines
--Cost Price is in inventory units, as will be the calculations, so no need for unit conversion
LEFT JOIN
	(SELECT	CP.ItemNumber
			, CP.UnitCode
			, CP.CompanyCode
			, CP.Price
			, CP.StartValidityDate
			, CP.EndValidityDate
			--Configuration:
			, PC.ProductConfigurationCode
			, PC.SiteCode
		FROM DataStore.CostPrice CP
		LEFT JOIN DataStore.ProductConfiguration PC
		ON CP.CompanyCode = PC.CompanyCode
			and CP.InventDimCode = PC.InventDimCode) CP1
ON NR.CompanyCode = CP1.CompanyCode
	AND CASE WHEN NR.RequirementDate = '1900-01-01' THEN CAST(current_timestamp() AS date) ELSE NR.RequirementDate END >= CP1.StartValidityDate --Initial Stock: Requirement date = 1900-01-01, hence no cost prices are available > USE CURRENT PRICE
	AND CASE WHEN NR.RequirementDate = '1900-01-01' THEN CAST(current_timestamp() AS date) ELSE NR.RequirementDate END <= CP1.EndValidityDate
	and NR.ProductCode = CP1.ItemNumber
	--and PO.InventUnit_UsedBOMLines = CP1.UnitId
	--Configuration (Depending on the level on which prices are calculated in AX)
	and PC1.ProductConfigurationCode = CP1.ProductConfigurationCode
	and PC1.SiteCode = CP1.SiteCode

--StockValue from AC to GC (as cost prices are denominated in AC):
--Required for currency conversion
INNER JOIN
	(
	SELECT	DISTINCT LES.AccountingCurrency
			, LES.ReportingCurrency
			, LES.`Name`
			, LES.ExchangeRateType
			, LES.BudgetExchangeRateType
			, G.GroupCurrencyCode AS GroupCurrency
	FROM dbo.SMRBILedgerStaging LES
	CROSS JOIN (SELECT TOP 1 /*FIXME*/ GroupCurrencyCode FROM ETL.GroupCurrency) G
	) L
ON NR.CompanyCode = L.`Name`


LEFT JOIN DataStore.ExchangeRate GC_CP -- GroupCurrency
ON GC_CP.FromCurrencyCode = L.AccountingCurrency
	and GC_CP.ToCurrencyCode = L.GroupCurrency
	and GC_CP.ExchangeRateTypeCode = L.ExchangeRateType
	and NR.RequirementDate BETWEEN GC_CP.ValidFrom AND GC_CP.ValidTo
;
