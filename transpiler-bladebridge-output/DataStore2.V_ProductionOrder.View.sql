/****** Object:  View [DataStore2].[V_ProductionOrder]    Script Date: 03/03/2026 16:26:09 ******/






CREATE OR REPLACE VIEW `DataStore2`.`V_ProductionOrder` AS 


SET (1,1,1,TotalEstimatedQuantity,TotalEstimatedQuantity,TotalEstimatedQuantity,TotalEstimatedQuantity,TotalEstimatedQuantity,TotalEstimatedQuantity,TotalEstimatedQuantity,TotalEstimatedQuantity,TotalEstimatedQuantity,TotalEstimatedQuantity,RankNr,TransactionDate,TotalQuantity,1,1,StatusIssue,StatusIssue,1,1,1,1,1,1,1,1,1,xRank,xRank,xRank,xRank,xRank,xRank,xRank,xRank,xRank,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,BOM,BOM,BOM,BOM,BOM,Resource_,TotalEstimatedQuantity,TotalEstimatedQuantity,TotalEstimatedQuantity,TotalEstimatedQuantity,TotalOriginalEstimatedQuantity,RankNr,DatePhysical,TotalQuantity,1,1,StatusIssue,StatusIssue) = (
WITH OTIF AS (

SELECT	PTS.ProdId AS ProductionOrderCode
	  , PTS.COMPANY AS CompanyCode
	  , CASE WHEN SUM(ITS.Qty) >= PTS.QTYSched THEN 1 ELSE SUM(ITS.Qty) / NULLIF(PTS.QTYSched, 0) END AS OTIF

FROM dbo.SMRBIProdTableStaging PTS

LEFT JOIN dbo.SMRBIInventTransOriginStaging ITOS
ON PTS.COMPANY = ITOS.COMPANY
--ON PTS.Company = ITOS.COMPANY
AND PTS.ProdId = ITOS.ReferenceId
AND PTS.ItemId = ITOS.ItemId

--AND PTS.InventDimId = ITS.ItemInventDimId
LEFT JOIN dbo.SMRBIInventTransStaging ITS
ON ITOS.InventTransOriginRecId = ITS.InventTransOrigin

WHERE 1=1
AND ITOS.ReferenceCategory = 2 --Production
AND (ITS.StatusIssue = 1 -- Sold
	OR ITS.StatusReceipt IN (1,2)) --Purchased/Received
AND ITS.DatePhysical <= DATEADD(DAY, 7-(EXTRACT(dw from PTS.SchedEnd)), PTS.SchedEnd) --Aggregate the produced quantities until EOW for a particular scheduled end date (against journal posting date)

GROUP BY PTS.ProdId
		, PTS.COMPANY
	   --, PTS.Company
	   , PTS.QTYSched
)
SELECT --Dimensions
		  COALESCE(NULLIF(PTS.ProdId, ''), '_N/A') AS ProductionOrderCode
		, 'Consumption' AS TransactionType
		, COALESCE(SM2.Name, '_N/A') AS ProductionOrderStatus
		, COALESCE(NULLIF(UPPER(PTS.Company), ''), '_N/A') AS CompanyCode
		, COALESCE(NULLIF(UPPER(PTS.ItemId), ''), '_N/A') AS ProductCode
		, COALESCE(NULLIF(PBS.ItemId, ''), '_N/A') AS ProductCode_UsedBOMLines --Required to retrieve Cost Prices
		, COALESCE(NULLIF(PTS.RouteId, ''), '_N/A') AS RouteCode
		, COALESCE(NULLIF(PC.SiteCode, ''), '_N/A') AS SiteCode --Required to join on DimRoute
		, COALESCE(NULLIF(PCTS.OprId, ''), '_N/A') AS OperationCode
		, COALESCE(NULLIF(PCTS.OprNum, ''), -1) AS OperationNumber --Required to join on DimOperations
		, COALESCE(NULLIF(PCTS.Resource_, ''), '_N/A') AS ResourceCode --Important for DWH.V_FactConsumedMaterials
		, COALESCE(NULLIF(SM.Name, ''), 0) AS CalculationType --Details the resource type that is used
		, COALESCE(NULLIF(PCTS.Resource_, ''), '_N/A') AS ProductionResourceCode --Can be an Ingredient, Worker, Machine (group)
		
		--Product Cost Breakdown:
		, COALESCE(NULLIF(PCTS.CostGroupId, ''), '_N/A') AS CostGroupCode
		
		--Technical Fields:
		, COALESCE(NULLIF(PTS.InventDimId, ''), '_N/A') AS InventDimCode --Required for the link with Product Configuration
		, COALESCE(NULLIF(PBS.InventDimCode, ''), '_N/A') AS InventDimCode_UsedBOMLines --Required to retrieve Cost Prices for the BOM Items
		, COALESCE(NULLIF(PTS.BOMId, ''), '_N/A') AS BOMCode
		
		--Dates:
		, COALESCE(NULLIF(PCTS.TransDate, ''), '1900-01-01') AS JournalPostingDate
		, COALESCE(NULLIF(PTS.DlvDate, ''), '1900-01-01') AS RequestedDeliveryDate
		, COALESCE(NULLIF(PTS.SchedEnd, ''), '1900-01-01') AS ScheduledDeliveryDate
		, COALESCE(NULLIF(PTS.FinishedDate, ''), '1900-01-01') AS ActualDeliveryDate
		, COALESCE(NULLIF(PTS.SchedStart, ''), '1900-01-01') AS ScheduledProductionStartDate
		, COALESCE(NULLIF(PTS.SchedEnd, ''), '1900-01-01') AS ScheduledProductionEndDate
		, COALESCE(NULLIF(PTS.StUpDate, ''), NULLIF(PRS.FromDate, ''), '1900-01-01') AS ProductionStartDate
		, COALESCE(NULLIF(PTS.FinishedDate, ''), NULLIF(PRS.ToDate, ''), '1900-01-01') AS ProductionEndDate
		
		--Measures: OTIF
		, COALESCE(O.OTIF, 0) AS OTIF --In case OTIF is not available from CTE, use 0 as not considered OTIF
		
		--Measures: Volume
		, COALESCE(NULLIF(ERRPS.InventoryUnitSymbol, ''), '_N/A') AS InventUnit
		, COALESCE(PBS.UnitId, '_N/A') AS InventUnit_UsedBOMLines --Required to retrieve Cost Prices for the BOM Items
		, 0 AS OriginalEstimatedQuantity--Estimated Quantity on HEADER will be retrieved in the union all
		, COALESCE(PTS.QtySched, 0) AS OriginalEstimatedQuantityDetail
		, 0 AS EstimatedQuantity--Estimated Quantity on HEADER will be retrieved in the union all
		, COALESCE(PTS.QtyStup, 0) AS EstimatedQuantityDetail
		, 0 AS ProducedQuantity--Produced Quantity on HEADER will be retrieved in the union all
		, COALESCE(ITS.TotalQuantity, 0) AS ProducedQuantityDetail
		, 0 AS ReportRemainderAsFinished--Report remainder as finished on HEADER will be retrieved in the union all
		, COALESCE(PTS.RemainInventPhysical, 0) AS ReportRemainderAsFinishedDetail
			
			/* Formula for Net Estimated Consumption:
				
				Note! the Unit of Measure is different for the different measures, hence conversion needs to be done

				Constant Consumption (incl. scrap)
					+ Variable Consumption (incl. Scrap)
					= Total Estimated Consumption
						
					- Constant Scrap
					/  (1 + Variable scrap (in Pct) / 100)
					
					= Net Estimated Consumption (excl. Scrap)
			*/
		, ROUND((COALESCE(PCTS.ConsumpConstant, 0) + COALESCE(PCTS.ConsumpVariable, 0)), 4) AS TotalEstimatedConsumptionQuantity --InventoryUnit
		, ROUND((COALESCE(PCTS.ConsumpConstant, 0) + COALESCE(PCTS.ConsumpVariable, 0) --InventoryUnit
			- COALESCE(CASE WHEN ERRPS1.BOMUnitSymbol = ERRPS1.InventoryUnitSymbol THEN PBS.ScrapConst ELSE PBS.ScrapConst * UOM1.Factor END, 0)) --Conversion needs to be done as Scrap is denominated in BOM Unit
			/ (1 + COALESCE(CASE WHEN ERRPS1.BOMUnitSymbol = ERRPS1.InventoryUnitSymbol THEN PBS.ScrapVar ELSE PBS.ScrapVar * UOM1.Factor END, 0) 
			/ 100), 4) AS NetEstimatedConsumptionQuantity --Conversion needs to be done as Scrap is denominated in BOM Unit
		, ROUND(COALESCE(PCTS.RealConsump, 0), 4) AS RealConsumptionQuantity --Inventory Unit
		, ROUND(COALESCE(CASE WHEN ERRPS1.BOMUnitSymbol = ERRPS1.InventoryUnitSymbol THEN PBS.ScrapConst ELSE PBS.ScrapConst * UOM1.Factor END, 0), 4) AS ConstantScrapQuantity  --Conversion needs to be done as Scrap is denominated in BOM Unit
		, (COALESCE(PCTS.ConsumpConstant, 0) + COALESCE(PCTS.ConsumpVariable, 0) - COALESCE(ScrapConst, 0)) --InventoryUnit
			- ROUND((COALESCE(PCTS.ConsumpConstant, 0) + COALESCE(PCTS.ConsumpVariable, 0) --InventoryUnit
			- COALESCE(CASE WHEN ERRPS1.BOMUnitSymbol = ERRPS1.InventoryUnitSymbol THEN PBS.ScrapConst ELSE PBS.ScrapConst * UOM1.Factor END, 0)) --Conversion needs to be done as Scrap is denominated in BOM Unit
			/ (1 + COALESCE(CASE WHEN ERRPS1.BOMUnitSymbol = ERRPS1.InventoryUnitSymbol THEN PBS.ScrapVar ELSE PBS.ScrapVar * UOM1.Factor END, 0) 
			/ 100), 4) AS VariableScrapQuantity--Expressed as actual quantity
		
		--Measures: €
		, ROUND(COALESCE(PCTS.CostAmount, 0) + COALESCE(PCTS.CostMarkup, 0), 4) AS TotalEstimatedCostAmountAC --Accounting Currency
		, CAST(0 AS DECIMAL(32,17)) AS NetEstimatedCostAmountAC --Net Estimated Cost Amount AC will be calculated in DataStore3
		, ROUND(COALESCE(PCTS.RealCostAmount, 0), 4) AS RealCostAmountAC

FROM dbo.SMRBIProdTableStaging PTS
--Join on OTIF
LEFT JOIN OTIF O
ON PTS.O.CompanyCode
AND PTS.O.ProductionOrderCode

--Join on Production Route to determine the actual start AND end date, in the case where the production route has not yet been completed
LEFT JOIN 
	(SELECT	ProdId
		  , COMPANY
		  , MIN(PRS.FromDate) AS FromDate
		  , MAX(PRS.ToDate) AS ToDate
	 FROM dbo.SMRBIProdRouteStaging PRS
	 WHERE 1
	 AND (PRS.FromDate != '1900-01-01'
	 OR PRS.ToDate != '1900-01-01')
	 GROUP BY ProdId, COMPANY
	) PRS
ON PTS.PRS.COMPANY
AND PTS.PRS.ProdId

-- Join to retrieve the production order details
LEFT JOIN dbo.SMRBIProdCalcTransStaging PCTS
ON PTS.PCTS.COMPANY
	and PTS.PCTS.TransRefId
	--and BOM = 0 --This is a line consumption (i.e. produced items)
	and PCTS.Resource_ != PTS.ItemId --The final produced item should only be retrieved in the second query

LEFT JOIN ETL.StringMap SM
ON SM.'SMRBIProdCalcTransStaging'
AND SM.'CalcType'
AND SM.PCTS.CalcType

--Join on Product Configuration to retrieve the Site Id
LEFT JOIN DataStore.ProductConfiguration PC
ON PC.PCTS.COMPANY
	and PC.PCTS.InventDimId

--Join to retrieve the ACTUAL produced quantity on line level
LEFT JOIN 

	(SELECT ITS.*
			, COALESCE(PTS.QtyStup, 0)
		FROM (SELECT ROW_NUMBER() OVER (PARTITION BY ITOS.COMPANY, ITS.ItemId, ITOS.ReferenceId ORDER BY ITOS.ReferenceId)
						, ITOS.COMPANY
						, ITS.ItemId
						, ITOS.ReferenceId
						, MIN(ITS.DatePhysical)
						, SUM(ITS.Qty)
				FROM dbo.SMRBIInventTransOriginStaging ITOS
				LEFT JOIN dbo.SMRBIInventTransStaging ITS
				ON ITOS.ITS.InventTransOrigin

				WHERE 1
					and ITOS.2 --Production
					and (1 -- Sold
						OR StatusReceipt IN (1,2)) --Purchased/Received

				GROUP BY ITOS.COMPANY
						, ITS.ItemId
						, ReferenceId) ITS 
		LEFT JOIN dbo.SMRBIProdTableStaging PTS
		ON ITS.PTS.Company
			and ITS.PTS.ProdId
			and ITS.1 --In order to avoid duplication of the estimated production quantity, put the value on the first line
			
	) ITS

ON PTS.ITS.COMPANY
	and PTS.ITS.ReferenceId
	and PTS.ITS.ItemId

--Join to calculate the total and net scrap 
LEFT JOIN 
	(SELECT	ROW_NUMBER() OVER (PARTITION BY BOMId, COMPANY, ItemId, ProdId, UnitId, PC.InventDimCode, PC.InventStatusCode, PC.ProductConfigurationCode, PC.SiteCode, PC.WarehouseCode ORDER BY BOMID, COMPANY, ItemId, ProdId, UnitId, PC.InventDimCode, PC.InventStatusCode, PC.ProductConfigurationCode, PC.SiteCode, PC.WarehouseCode ASC) AS xRank
			, BOMId
			, COMPANY
			, ItemId
			, ProdId
			, UnitId
			, ScrapConst
			, ScrapVar
			--Product Configuration:
			, PC.InventDimCode
			, PC.InventStatusCode
			, PC.ProductConfigurationCode
			, PC.SiteCode
			, PC.WarehouseCode
		
		FROM dbo.SMRBIProdBOMStaging PBS
		
		LEFT JOIN DataStore.ProductConfiguration PC 
		ON PBS.PC.CompanyCode
			and PBS.PC.InventDimCode

		WHERE 1
		) PBS
ON PTS.PBS.COMPANY
AND PTS.PBS.BOMId
AND PTS.PBS.ProdId
AND PCTS.PBS.ItemId
--AND PBS.InventDimId = PCTS.InventDimId --No join on InventDim, only on some of the dimensions (NICOLS specific, check!)
AND PC.PBS.InventStatusCode
AND PC.PBS.ProductConfigurationCode
AND PC.PBS.SiteCode
AND PC.PBS.WarehouseCode
AND 1 --There is a possibility there are multiple BOM lines with the same characteristics (it is correct to use the 1st one, as per discussion Dirk Bulckaen)
	
--Join to retrieve the UOM for the produced items
LEFT JOIN dbo.SMRBIEcoResReleasedProductStaging ERRPS
ON ERRPS.PTS.Company
AND ERRPS.PTS.ItemId

--Join to retrieve the UOM for the BOM items
LEFT JOIN dbo.SMRBIEcoResReleasedProductStaging ERRPS1
ON ERRPS1.PBS.COMPANY
AND ERRPS1.PBS.ItemId

--Join to convert the BOM Unit to the Inventory Unit for the BOM lines
LEFT JOIN DataStore.UnitOfMeasure UOM1
ON UOM1.PTS.Company
AND UOM1.PBS.ItemId
AND UOM1.ERRPS1.BOMUnitSymbol
AND UOM1.ERRPS1.InventoryUnitSymbol

LEFT JOIN ETL.StringMap SM2
ON SM2.'D365FO'
AND SM2.'ProdTable'
AND SM2.'ProdStatus'
AND SM2.PTS.ProdStatus

WHERE 1

UNION ALL

SELECT --Dimensions
		COALESCE(NULLIF(PTS.ProdId, ''), '_N/A') AS ProductionOrderCode
		, 'Production' AS TransactionType
		, COALESCE(SM2.Name, '_N/A') AS ProductionOrderStatus
		, COALESCE(NULLIF(PTS.Company, ''), '_N/A') AS CompanyCode
		, COALESCE(NULLIF(UPPER(PTS.ItemId), ''), '_N/A') AS ProductCode
		, '_N/A' AS ProductCode_UsedBOMLines
		, COALESCE(NULLIF(PTS.RouteId, ''), '_N/A') AS RouteCode
		, COALESCE(NULLIF(PC.SiteCode, ''), '_N/A') AS SiteCode --Required to join on DimRoute
		, COALESCE(NULLIF(PCTS.OprId, ''), '_N/A') AS OperationCode
		, COALESCE(NULLIF(PCTS.OprNum, ''), -1) AS OperationNumber --Required to join on DimRoute
		, COALESCE(NULLIF(PCTS.Resource_, ''), '_N/A') AS ResourceCode --Important for DWH.V_FactConsumedMaterials
		, COALESCE(NULLIF(SM.Name, ''), 0) AS CalculationType --Details the resource type that is used
		, COALESCE(NULLIF(PCTS.Resource_, ''), '_N/A') AS ProductionResourceCode --Can be an Ingredient, Worker, Machine (group)
		--Product Cost Breakdown:
		, COALESCE(NULLIF(PCTS.CostGroupId, ''), '_N/A') AS CostGroupCode
		--Technical Fields:
		, COALESCE(NULLIF(PTS.InventDimId, ''), '_N/A') AS InventDimCode --Required for the link with Product Configuration
		, '_N/A' AS InventDimCode_UsedBOMLines
		, COALESCE(NULLIF(PTS.BOMId, ''), '_N/A') AS BOMCode
		--Dates:
		, COALESCE(NULLIF(ITS.DatePhysical, ''), '1900-01-01') AS JournalPostingDate
		, COALESCE(NULLIF(PTS.DlvDate, ''), '1900-01-01') AS RequestedDeliveryDate
		, COALESCE(NULLIF(PTS.SchedEnd, ''), '1900-01-01') AS ScheduledDeliveryDate
		, COALESCE(NULLIF(PTS.FinishedDate, ''), '1900-01-01') AS ActualDeliveryDate
		, COALESCE(NULLIF(PTS.SchedStart, ''), '1900-01-01') AS ScheduledProductionStartDate
		, COALESCE(NULLIF(PTS.SchedEnd, ''), '1900-01-01') AS ScheduledProductionEndDate
		, COALESCE(NULLIF(PTS.StUpDate, ''), NULLIF(PRS.FromDate, ''), '1900-01-01') AS ProductionStartDate
		, COALESCE(NULLIF(PTS.FinishedDate, ''), NULLIF(PRS.ToDate, ''), '1900-01-01') AS ProductionEndDate
		--Measures: OTIF
		, COALESCE(O.OTIF, 0) AS OTIF --In case OTIF is not available from CTE, use 0 as not considered OTIF
		--Measures: Volume
		, COALESCE(NULLIF(ERRPS.InventoryUnitSymbol, ''), '_N/A') AS InventUnit
		, '_N/A' AS InventUnit_UsedBOMLines
		, COALESCE(ITS.TotalOriginalEstimatedQuantity, 0) AS OriginalEstimatedQuantity
		, CAST(0 AS DECIMAL(38,17)) AS OriginalEstimatedQuantityDetail
		, COALESCE(ITS.TotalEstimatedQuantity, 0) AS EstimatedQuantity
		, CAST(0 AS DECIMAL(38,17)) AS EstimatedQuantityDetail
		, COALESCE(ITS.TotalQuantity, 0) AS ProducedQuantity
		, CAST(0 AS DECIMAL(38,17)) AS ProducedQuantityDetail
		, COALESCE(PTS.RemainInventPhysical, 0) AS ReportRemainderAsFinished
		, CAST(0 AS DECIMAL(38,17)) AS ReportRemainderAsFinishedDetail
		--Measures: €
		, CAST(0 AS DECIMAL(32,17)) AS TotalEstimatedConsumptionQuantity
		, CAST(0 AS DECIMAL(32,17)) AS NetEstimatedConsumptionQuantity
		, CAST(0 AS DECIMAL(32,17)) AS RealConsumptionQuantity
		, CAST(0 AS DECIMAL(32,17)) AS ConstantScrapQuantity
		, CAST(0 AS DECIMAL(32,17)) AS VariableScrapQuantity
		, CAST(0 AS DECIMAL(32,17)) AS TotalEstimatedCostAmountAC
		, CAST(0 AS DECIMAL(32,17)) AS NetEstimatedCostAmountAC --Net Estimated Cost Amount AC will be calculated in DataStore3
		, CAST(0 AS DECIMAL(32,17)) AS RealCostAmountAC	

FROM dbo.SMRBIProdTableStaging PTS

--Join on OTIF
LEFT JOIN OTIF O
ON PTS.O.CompanyCode
AND PTS.O.ProductionOrderCode

--Join on Production Route to determine the actual start and end date, in the case where the production route has not yet been completed
LEFT JOIN 
	(SELECT	ProdId
		  , COMPANY
		  , MIN(PRS.FromDate) AS FromDate
		  , MAX(PRS.ToDate) AS ToDate
		FROM dbo.SMRBIProdRouteStaging PRS
		WHERE 1
		AND (PRS.FromDate != '1900-01-01'
		OR PRS.ToDate != '1900-01-01')
		GROUP BY ProdId, COMPANY
	) PRS
ON PTS.PRS.COMPANY
AND PTS.PRS.ProdId

-- Join to retrieve the production order details
LEFT JOIN dbo.SMRBIProdCalcTransStaging PCTS
ON PTS.PCTS.COMPANY
AND PTS.PCTS.TransRefId
AND 1 --This is a line consumption (i.e. produced items)
AND PTS.ItemId --Only the final produced item should be retrieved in the second query

LEFT JOIN ETL.StringMap SM
ON SM.'SMRBIProdCalcTransStaging'
AND SM.'CalcType'
AND SM.PCTS.CalcType

--Join to retrieve the ACTUAL produced quantity
LEFT JOIN 

	(SELECT ITS.*
			, COALESCE(PTS.QtyStup, 0)
			, COALESCE(PTS.QtySched, 0)
		FROM dbo.SMRBIProdTableStaging PTS
			LEFT JOIN (SELECT ROW_NUMBER() OVER (PARTITION BY ITOS.COMPANY, ITS.ItemId, ITOS.ReferenceId ORDER BY ITOS.ReferenceId ASC)
						, ITOS.COMPANY
						, ITS.ItemId
						, ITOS.ReferenceId
						, MAX(ITS.DatePhysical)
						, SUM(ITS.Qty)
				FROM dbo.SMRBIInventTransOriginStaging ITOS 
			LEFT JOIN dbo.SMRBIInventTransStaging ITS
			ON ITOS.ITS.InventTransOrigin
			WHERE 1
				and ITOS.2 --Production
				and (1 -- Sold
					OR StatusReceipt IN (1,2)) --Purchased/Received

			GROUP BY ITOS.COMPANY
					, ITS.ItemId
					, ReferenceId
					--, DatePhysical
			) ITS 
			ON ITS.COMPANY = PTS.Company
				and ITS.ReferenceId = PTS.ProdId

	) ITS

ON PTS.Company = ITS.COMPANY
	and PTS.ProdId = ITS.ReferenceId
	and PTS.ItemId = ITS.ItemId

--Join on Product Configuration to retrieve the Site Id
LEFT JOIN DataStore.ProductConfiguration PC
ON PC.CompanyCode	= PCTS.COMPANY
AND PC.InventDimCode = PCTS.InventDimId

--Join to retrieve the UOM for the produced items
LEFT JOIN dbo.SMRBIEcoResReleasedProductStaging ERRPS
ON ERRPS.COMPANY = PTS.Company
AND ERRPS.ItemNumber = PTS.ItemId

LEFT JOIN ETL.StringMap SM2
ON SM2.SourceSystem = 'D365FO'
AND SM2.SourceTable = 'ProdTable'
AND SM2.SourceColumn = 'ProdStatus'
AND SM2.Enum = PTS.ProdStatus


WHERE 1=1
;
);
