/****** Object:  View [DWH].[V_FactNetRequirements]    Script Date: 03/03/2026 16:26:08 ******/










CREATE OR REPLACE VIEW `DWH`.`V_FactNetRequirements` AS 


SELECT	  UPPER(NR.CompanyCode) AS CompanyCode
		, UPPER(NR.ProductCode) AS ProductCode
		, UPPER(NR.InventDimCode) AS InventDimCode
		, UPPER(NR.ReferenceCode) AS ReferenceCode
		, UPPER(NR.ProducedItemCode) AS ProducedItemCode
		, UPPER(NR.CustomerCode) AS CustomerCode
		, UPPER(NR.VendorCode) AS VendorCode
		, ETL.fn_DateKeyInt(NR.RequirementDate) AS DimRequirementDateId
		, CAST(RequirementDate AS TIMESTAMP) AS RequirementDateTime
		, RequirementDateTime AS RequirementDateTimeTime
		, RequirementTime
		, NR.ActionDate
		, NR.ReferenceType
		, NR.RankNr
		, NR.PlanVersion
		, NR.ActionDays
		, NR.ActionType
		, NR.ActionMarked
		, NR.FuturesDate
		, NR.FuturesDays
		, NR.FuturesCalculated
		, NR.FuturesMarked
		, NR.Direction
		, NR.FictionalOOS
		, NR.FictionalOOS_Confirmed
		, NR.ActualOOS
		, NR.ActualOOS_Confirmed
		, NR.Quantity_InventoryUnit
		, NR.Quantity_SalesUnit
		, NR.Quantity_PurchaseUnit
		, NR.AccumulatedQuantity_InventoryUnit
		, NR.AccumulatedQuantity_SalesUnit
		, NR.AccumulatedQuantity_PurchaseUnit
		, NR.QuantityConfirmed_InventoryUnit
		, NR.QuantityConfirmed_SalesUnit
		, NR.QuantityConfirmed_PurchaseUnit
		, NR.AccumulatedQuantityConfirmed_InventoryUnit
		, NR.AccumulatedQuantityConfirmed_SalesUnit
		, NR.AccumulatedQuantityConfirmed_PurchaseUnit
		, CAST(COALESCE(PC.ProductCostPriceAC, 0) AS decimal(24,15)) AS ProductCostPriceAC
		, CAST(COALESCE(PC.ProductCostPriceGC, 0) AS decimal(24,15)) AS ProductCostPriceGC
		, ValueAC
		, ValueGC


FROM DataStore2.NetRequirements NR

LEFT JOIN 
	(
	SELECT    ProductCode
			, CompanyCode
			, IsMaxPrice
			, IsActivePrice
			, AVG(ProductCostPriceAC) AS ProductCostPriceAC
			, AVG(ProductCostPriceGC) AS ProductCostPriceGC

	FROM DataStore6.ProductCostBreakdownTheoretical

	GROUP BY  ProductCode, CompanyCode, IsMaxPrice, IsActivePrice

	HAVING IsMaxPrice = 'Yes' AND IsActivePrice = 'Yes'

	) PC
      ON NR.CompanyCode = PC.CompanyCode
      AND NR.ProductCode = PC.ProductCode
;
