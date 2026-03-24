/****** Object:  View [DWH].[V_DIO1]    Script Date: 03/03/2026 16:26:08 ******/







CREATE OR REPLACE VIEW `DWH`.`V_DIO1` AS 

/************************************************************************************************************************************************************

Purpose: determine the total product quantities/balance of sales invoices (FactSalesInvoice) and the stock amount (FactInventory) during a particular month,
		 for a particular customer

Note! Everything will be denominated in ACCOUNTING CURRENCY and INVENTORY UNITS

Steps:
	1) Count the number of (working) days in a certain month
	2) Take the sum of the gross sales volume and value, per month, per product
		2a) Determine the sales/inventory units of the transaction
		2b) Determine the conversion factor from sales > inventory unit to determine the sales volume/value in inventory unit
		2c) Determine the COST price of the sales in case there is a price for the sales unit
		2d) Determine the COST price of the sales in case there is no price for the sales unit

	3) Take the snapshot of the inventory, per month, per product
		3a) Determine the latest inventory snapshot date (does not have to be the EOM)
		3b) Determine the stock volume and stock value, per month, per product

	4) Take the sum of the consumed Products (Productionresources) and values, per month, per product

	5) Collate all information

************************************************************************************************************************************************************/

--Count the number of (working) days in a certain month
;
WITH CalendarDays AS (
	SELECT	  D1.MonthId
			, D1.FirstDayOfMonth
			, D1.LastDayOfMonth
			, D1.FirstDayOfYear
			, D1.LastDayOfYear
			, WorkingDays = COUNT(D2.DimDateId)
			, CalendarDays = COUNT(D1.DimDateId)
	FROM (
		SELECT	*
				, FirstDayOfMonth = CAST(DATEADD(month, CAST(MONTHS_BETWEEN(0, TIMESTAMP) AS INT), 0) AS TIMESTAMP)
				, LastDayOfMonth = CAST(DATEADD(SECOND, -1, DATEADD(MONTH, CAST(MONTHS_BETWEEN(0, TIMESTAMP) AS INT)+1, 0)) AS TIMESTAMP)
				, FirstDayOfYear = CAST(DATEADD(YEAR, CAST(DATEDIFF(0, TIMESTAMP) / 365 AS INT), 0) AS TIMESTAMP)
				, LastDayOfYear = CAST(DATEADD(YEAR, CAST(DATEDIFF(0, TIMESTAMP) / 365 AS INT) + 1, -1) AS TIMESTAMP)
		FROM DWH.DimDate) D1
	LEFT JOIN DWH.DimDate D2
	ON D1.DimDateId = D2.DimDateId
		and D2.DayOfWeekName NOT IN ('Saturday', 'Sunday')

	WHERE 1=1
		and D1.YearId >= (SELECT YearId - 2 FROM DWH.DimDate WHERE TIMESTAMP IN (SELECT CAST(CAST(current_timestamp() AS date) AS TIMESTAMP))) --Go back MAX 2 years
		and D1.MonthId < (SELECT MonthId FROM DWH.DimDate WHERE TIMESTAMP = CAST(CAST(current_timestamp() AS date) AS TIMESTAMP)) --Exclude current month

	GROUP BY D1.MonthId, D1.FirstDayOfMonth, D1.LastDayOfMonth, D1.FirstDayOfYear, D1.LastDayOfYear
),

--Take the sum of the gross sales volume and value, per month, per product
Turnover AS (
	
	SELECT	  DimProductId
			, CompanyCode
			, MonthId
			, SalesVolume = SUM(SalesVolume)
			, SalesValue = SUM(SalesValue) --Denominated in accounting currency and sales units!
	FROM
		(SELECT DP.DimProductId
				, DP.CompanyCode
				, MonthId = COALESCE(DD.MonthId, CAST((CAST(YEAR(current_timestamp()) AS STRING) + RIGHT('0'||CAST(MONTH(current_timestamp()) AS STRING),2)) AS int))
				, SalesVolume = SUM(COALESCE(InvoicedQuantity_SalesUnit, 0) 
									* CASE WHEN ITM_S.UnitId = ITM_I.UnitId THEN 1 ELSE  COALESCE(UOM.Factor, 0) END) -- Conversion factor Sales > Inventory unit
				, SalesValue = SUM(COALESCE(InvoicedQuantity_SalesUnit, 0)
									* COALESCE(CP1.Price, CP2.Price, 0) --Determine the cost price of the item (accounting currency)
									* CASE WHEN ITM_S.UnitId = ITM_I.UnitId THEN 1 ELSE  COALESCE(UOM.Factor, 0) END) -- Conversion factor Sales > Inventory unit
		FROM DWH.DimProduct DP
		LEFT JOIN DWH.FactSalesInvoice FSI
		ON DP.DimProductId = FSI.DimProductId
		LEFT JOIN DWH.DimDate DD
		ON FSI.DimInvoiceDateId = DD.DimDateId		
		
		LEFT JOIN DataStore.ProductConfiguration PC
		ON PC.CompanyCode = DP.CompanyCode
			and PC.InventDimCode = FSI.InventDimCode

		--Determine the SALES UNIT
		LEFT JOIN (SELECT DISTINCT * FROM dbo.SMRBIInventTableModuleStaging) ITM_S
		ON DP.CompanyCode = ITM_S.DataAreaId
			and DP.ProductCode = ITM_S.ItemId
			and ITM_S.ModuleType = 2 --Sales
		--Determine the INVENTORY UNIT
		LEFT JOIN (SELECT DISTINCT * FROM dbo.SMRBIInventTableModuleStaging) ITM_I
		ON DP.CompanyCode = ITM_I.DataAreaId
			and DP.ProductCode = ITM_I.ItemId
			and ITM_I.ModuleType = 0 --Inventory
		--Determine the conversion factor from SALES to INVENTORY units
		LEFT JOIN DataStore.UnitOfMeasure UOM --where FromUOM = ToUOM
		ON DP.ProductCode = UOM.Product
			and DP.CompanyCode = UOM.CompanyCode
			and UOM.FromUOM = ITM_S.UnitId
			and UOM.ToUOM = ITM_I.UnitId
		
		--In case there is a price for the sales unit of the item
		LEFT JOIN 
			(SELECT DISTINCT ItemNumber
					, UnitCode
					, CP.CompanyCode
					, ProductConfigurationCode
					, Price
					, StartValidityDate
					, EndValidityDate
				FROM DataStore.CostPrice CP
				JOIN DataStore.ProductConfiguration PC
				ON CP.CompanyCode = PC.CompanyCode
					and CP.InventDimCode = PC.InventDimCode) CP1
		ON DP.ProductCode = CP1.ItemNumber
			and DP.CompanyCode = CP1.CompanyCode
			and ITM_S.UnitId = CP1.UnitCode
			and DD.TIMESTAMP >= CP1.StartValidityDate
			and DD.TIMESTAMP <= CP1.EndValidityDate
			and PC.ProductConfigurationCode = CP1.ProductConfigurationCode --Note! Determine at which level prices are set (Include ProductConfiguration in the join)

		--In case no cost price exists for the sales unit, convert the cost price unit to the applicable sales units
		LEFT JOIN 
			(SELECT	  CP.CompanyCode
					, CP.ItemNumber
					, CP.ProductConfigurationCode
					, CP.StartValidityDate
					, CP.EndValidityDate
					, CostPrice = CP.Price
					, CostPriceUnit = CP.UnitCode
					, Separator = '***'
					, Price = CP.Price / UOM1.Factor
					, UOM1.Factor
					, UOM1.FromUOM
					, ConversionUnit = UOM1.ToUOM
				FROM
					(SELECT DISTINCT ItemNumber
						, UnitCode
						, CP.CompanyCode
						, ProductConfigurationCode
						, Price
						, StartValidityDate
						, EndValidityDate
					FROM DataStore.CostPrice CP
					JOIN DataStore.ProductConfiguration PC
					ON CP.CompanyCode = PC.CompanyCode
						and CP.InventDimCode = PC.InventDimCode) CP
				LEFT JOIN DataStore.UnitOfMeasure UOM1
				ON CP.ItemNumber = UOM1.Product
					and CP.CompanyCode = UOM1.CompanyCode
					and CP.UnitCode = UOM1.FromUOM
					
			) CP2

		ON DP.CompanyCode = CP2.CompanyCode
			and DD.TIMESTAMP >= CP2.StartValidityDate
			and DD.TIMESTAMP <= CP2.EndValidityDate
			and ITM_S.UnitId = CP2.ConversionUnit
			and DP.ProductCode = CP2.ItemNumber
			and PC.ProductConfigurationCode = CP2.ProductConfigurationCode --Note! Determine at which level prices are set (Include ProductConfiguration in the join)

		WHERE 1=1
			and DP.CompanyCode != 'GROUP'
			--and dp.CompanyId = 'NINT'
			--and dp.ProductCode = '500800'
			--and DP.DimProductId = 486 --> Temporary code: REMOVE !!!
		GROUP BY DP.DimProductId
					, DD.MonthId
					, DP.CompanyCode) T
	GROUP BY DimProductId, CompanyCode, MonthId
	--ORDER BY DimProductId, MonthId --> Temporary code: REMOVE !!!

),

--Determine the last inventory snapshot date, per month (required in next step)
MaxInventoryDate AS (
	SELECT	DD.MonthId
			, MaxInventoryDate = MAX(I.DimReportDateId)
		FROM DWH.FactInventory I
		JOIN DWH.DimDate DD
		ON I.DimReportDateId = DD.DimDateId
		GROUP BY DD.MonthId
),

--Determine the stock volume and stock value, per month, per product
Stock AS (
	SELECT	DimProductId = DP.DimProductId
			, MonthId = COALESCE(I.MonthId, CAST((CAST(YEAR(current_timestamp()) AS STRING) + RIGHT('0'||CAST(MONTH(current_timestamp()) AS STRING),2)) AS int))
			, StockVolume = SUM(COALESCE(I.StockQuantity_InventoryUnit, 0))
			, StockValue = SUM(COALESCE(I.StockValueAC, 0))
			, DP.CompanyCode
	FROM DWH.DimProduct DP
	LEFT JOIN 
		(SELECT	  I.DimProductId
				, I.DimReportDateId
				, MID.MonthId
				, I.StockQuantity_InventoryUnit
				, I.InventoryUnit
				, I.StockValueAC
			FROM DWH.FactInventory I
			JOIN MaxInventoryDate MID --For the date, take the last available date (not necessarily EOM!)
			ON I.DimReportDateId = MID.MaxInventoryDate) I
	ON DP.DimProductId = I.DimProductId

	WHERE 1=1
		and DP.CompanyCode != 'GROUP'
		--and DP.DimProductId = 3401 --> Temporary code: REMOVE !!!
	GROUP BY DP.DimProductId, I.MonthId, DP.CompanyCode
	--ORDER BY DD.MonthId --> Temporary code: REMOVE !!!
),

--Determine the used raw materials, by joining later on to DimProduct, machines etc will be filtered out
Production AS (
	SELECT 
		  ProductionResourceCode
		, PO.DimcompanyId
		, ProductionStartMonth = D.MonthId
		, ConsumedVolume = COALESCE(SUM(RealConsumptionQuantity_InventoryUnit), 0)
		, ConsumedValue =  COALESCE(SUM(RealCostAmountGC), 0)
	FROM DWH.FactProductionOrder PO 
	LEFT JOIN DWH.DimDate D
	ON PO.DimProductionStartDateId = D.DimDateId
	
	GROUP BY ProductionResourceCode, PO.DimcompanyId, D.MonthId
	--Should not be filtered for resource as this will be joined to the product dimension

)

--Select statement:

SELECT	  DP.DimProductId
		, C.DimCompanyId
		, DD.MonthId
		, WD.CalendarDays
		, SalesVolume = COALESCE(T.SalesVolume, 0) + COALESCE(P.ConsumedVolume, 0)
		, SalesValue = COALESCE(T.SalesValue, 0) + COALESCE(P.ConsumedValue, 0)
		, StockVolume = COALESCE(S.StockVolume, 0)
		, StockValue = COALESCE(S.StockValue, 0)

FROM DWH.DimProduct DP
CROSS JOIN (SELECT DISTINCT MonthId, LastDayOfMonth FROM CalendarDays) DD --Limit to active months
LEFT JOIN Turnover T
ON T.DimProductId = DP.DimProductId
	and T.MonthId = DD.MonthId
	and T.CompanyCode = DP.CompanyCode
LEFT JOIN CalendarDays WD
ON WD.MonthId = DD.MonthId
LEFT JOIN Stock S
ON DP.DimProductId = S.DimProductId 
	and S.MonthId = DD.MonthId
	and DP.CompanyCode = S.CompanyCode
LEFT JOIN DWH.DimCompany C
ON DP.CompanyCode = C.CompanyCode
LEFT JOIN Production P 
ON DP.ProductCode = P.ProductionResourceCode
	AND C.DimCompanyId = P.DimCompanyId
	AND DD.MonthId = P.ProductionStartMonth
WHERE 1=1
	and DD.MonthId != 190001 
	and DD.MonthId < CAST((CAST(YEAR(current_timestamp()) AS STRING) + RIGHT('0'||CAST(MONTH(current_timestamp()) AS STRING),2)) AS int) --Do (not) include the current month
	and DP.CompanyCode != 'GROUP'
	--and dp.companyID = 'NINT'
	--and dp.ProductCode = '500800'


	--and DP.DimProductId IN (486) --> Temporary code: REMOVE !!!

--ORDER BY DP.DimProductId, DD.MonthId --> Temporary code: REMOVE !!!
;
