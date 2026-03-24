/****** Object:  View [DataStore6].[V_ProductCostBreakdownTheoretical]    Script Date: 03/03/2026 16:26:08 ******/











CREATE OR REPLACE VIEW `DataStore6`.`V_ProductCostBreakdownTheoretical` AS 

/* The following hierarchy is dependent on the levels defined in the costing sheet, which are subsequently loaded using the procedure ETL.CostSheetNodeHierarchy */
;
SET (Resource_,GroupCurrency,GroupCurrency,GroupCurrency,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,Level_2_Code) = (
WITH CostSheetNodeHierarchyTable AS (

	SELECT	DISTINCT DataAreaId
			, Level_1_Code
			, Level_1_Description
			, Level_1_CostGroupId
			, Level_2_Code
			, Level_2_Description
			, Level_2_CostGroupId
			, Level_3_Code
			, Level_3_Description
			, Level_3_CostGroupId
			--, Level_4_Code --> Check if this needs to be added!
			--, Level_4_Description --> Check if this needs to be added!
			--, Level_4_CostGroupId --> Check if this needs to be added!
			--, Level_5_Code --> Check if this needs to be added!
			--, Level_5_Description --> Check if this needs to be added!
			--, Level_5_CostGroupId --> Check if this needs to be added!

		FROM dbo.CostSheetNodeHierarchyTable

	)
SELECT	  BOMCalcTransRecId
		, PCB.CompanyCode
		, FinishedProductCode
		, PCB.ProductCode
		, InventDimCode
		, CalculationType
		, PriceType
		, CalculationCode
		, IsCalculatedPrice
		, IsMaxCalculation
		, IsMaxPrice
		, Levelling
		, CAST(PCB.Resource_ + CASE WHEN P.ProductName is not NULL THEN ' - ' || UPPER(P.ProductName) ELSE '' END AS STRING)
		, CostGroupCode
		, CostPriceUnitSymbol
		, CostPriceCalculationNumber
		, CostPriceModel
		, CostPriceVersion
		, CostPriceType
	--Date information:
		, CalculationDate
		, IsActivePrice

	--Currency information:
		, COALESCE(L.AccountingCurrency, '_N/A') AS AccountingCurrencyCode
		, COALESCE(L.ReportingCurrency, '_N/A') AS ReportingCurrencyCode
		, COALESCE(L.GroupCurrency, '_N/A') AS GroupCurrencyCode

		, COALESCE(L.ExchangeRateType, '_N/A') AS DefaultExchangeRateTypeCode
		, COALESCE(L.BudgetExchangeRateType, '_N/A') AS BudgetExchangeRateTypeCode
	
	--Levels:
		, COALESCE(COALESCE(NULLIF(COALESCE(CSNHT1.Level_1_Code, CSNHT2.Level_1_Code, CSNHT3.Level_1_Code), ''), '_N/A') + CASE WHEN COALESCE(CSNHT1.Level_1_Description, CSNHT2.Level_1_Description, CSNHT3.Level_1_Description) = '' THEN '' ELSE ' | ' || COALESCE(CSNHT1.Level_1_Description, CSNHT2.Level_1_Description, CSNHT3.Level_1_Description) END, '_N/A') AS Level_1
		, COALESCE(COALESCE(NULLIF(COALESCE(CSNHT1.Level_2_Code, CSNHT2.Level_2_Code, CSNHT3.Level_2_Code), ''), '_N/A') + CASE WHEN COALESCE(CSNHT1.Level_2_Description, CSNHT2.Level_2_Description, CSNHT3.Level_2_Description) = '' THEN '' ELSE ' | ' || COALESCE(CSNHT1.Level_2_Description, CSNHT2.Level_2_Description, CSNHT3.Level_2_Description) END, '_N/A') AS Level_2
		, COALESCE(COALESCE(NULLIF(COALESCE(CSNHT1.Level_3_Code, CSNHT2.Level_3_Code, CSNHT3.Level_3_Code), ''), '_N/A') + CASE WHEN COALESCE(CSNHT1.Level_3_Description, CSNHT2.Level_3_Description, CSNHT3.Level_3_Description) = '' THEN '' ELSE ' | ' || COALESCE(CSNHT1.Level_3_Description, CSNHT2.Level_3_Description, CSNHT3.Level_3_Description) END, '_N/A') AS Level_3
		--, Level_4 = ISNULL(ISNULL(NULLIF(COALESCE(CSNHT1.Level_4_Code, CSNHT2.Level_4_Code, CSNHT3.Level_4_Code), ''), '_N/A') + CASE WHEN COALESCE(CSNHT1.Level_4_Description, CSNHT2.Level_4_Description, CSNHT3.Level_4_Description) = '' THEN '' ELSE ' | ' + COALESCE(CSNHT1.Level_4_Description, CSNHT2.Level_4_Description, CSNHT3.Level_4_Description) END, '_N/A') --> Check if this needs to be added!
		--, Level_5 = ISNULL(ISNULL(NULLIF(COALESCE(CSNHT1.Level_5_Code, CSNHT2.Level_5_Code, CSNHT3.Level_5_Code), ''), '_N/A') + CASE WHEN COALESCE(CSNHT1.Level_5_Description, CSNHT2.Level_5_Description, CSNHT3.Level_5_Description) = '' THEN '' ELSE ' | ' + COALESCE(CSNHT1.Level_5_Description, CSNHT2.Level_5_Description, CSNHT3.Level_5_Description) END, '_N/A') --> Check if this needs to be added!
		--, Level_6 = ISNULL(ISNULL(NULLIF(COALESCE(CSNHT1.Level_6_Code, CSNHT2.Level_6_Code, CSNHT3.Level_6_Code), ''), '_N/A') + CASE WHEN COALESCE(CSNHT1.Level_6_Description, CSNHT2.Level_6_Description, CSNHT3.Level_6_Description) = '' THEN '' ELSE ' | ' + COALESCE(CSNHT1.Level_6_Description, CSNHT2.Level_6_Description, CSNHT3.Level_6_Description) END, '_N/A')
		 --> Check if this needs to be added!
		, COALESCE(PCB.ProductCostPrice, 0) AS ProductCostPriceAC
		, COALESCE(CASE WHEN L.AccountingCurrency = L.ReportingCurrency 
THEN CAST(COALESCE(PCB.ProductCostPrice, 0) AS decimal(20,10))  
ELSE (COALESCE(PCB.ProductCostPrice, 0)) * CAST(RC.ExchangeRate AS decimal(10,7)) 
END, 0) ProductCostPriceRC 
		, COALESCE(CASE WHEN L.AccountingCurrency = L.GroupCurrency 
THEN CAST(COALESCE(PCB.ProductCostPrice, 0) AS decimal(20,10))  
ELSE (COALESCE(PCB.ProductCostPrice, 0)) * CAST(GC.ExchangeRate AS decimal(10,7)) 
END, 0) AS ProductCostPriceGC
		, COALESCE(CASE WHEN L.ReportingCurrency = L.GroupCurrency THEN CAST(COALESCE(PCB.ProductCostPrice, 0) AS decimal(20,10))  ELSE (COALESCE(PCB.ProductCostPrice, 0)) * CAST(RC_Budget.ExchangeRate AS decimal(10,7)) END, 0) AS ProductCostPriceRC_Budget
		, COALESCE(CASE WHEN L.AccountingCurrency = L.GroupCurrency THEN CAST(COALESCE(PCB.ProductCostPrice, 0) AS decimal(20,10))  ELSE (COALESCE(PCB.ProductCostPrice, 0)) * CAST(GC_Budget.ExchangeRate AS decimal(10,7)) END, 0) AS ProductCostPriceGC_Budget 

		, COALESCE(PCB.ComponentCostPrice, 0) AS ComponentCostPriceAC
		, COALESCE(CASE WHEN L.ReportingCurrency = L.GroupCurrency 
THEN (COALESCE(PCB.ComponentCostPrice, 0))  
ELSE CAST(COALESCE(PCB.ComponentCostPrice, 0) AS decimal(20,10)) * CAST(RC.ExchangeRate AS decimal(10,7)) 
END, 0) AS ComponentCostPriceRC 
		, COALESCE(CASE WHEN L.AccountingCurrency = L.GroupCurrency 
THEN (COALESCE(PCB.ComponentCostPrice, 0))  
ELSE CAST(COALESCE(PCB.ComponentCostPrice, 0) AS decimal(20,10)) * CAST(GC.ExchangeRate AS decimal(10,7)) 
END, 0) AS ComponentCostPriceGC
		, COALESCE(CASE WHEN L.ReportingCurrency = L.GroupCurrency 
THEN (COALESCE(PCB.ComponentCostPrice, 0))  
ELSE CAST(COALESCE(PCB.ComponentCostPrice, 0) AS decimal(20,10)) * CAST(RC_Budget.ExchangeRate AS decimal(10,7)) 
END, 0) AS ComponentCostPriceRC_Budget
		, COALESCE(CASE WHEN L.AccountingCurrency = L.GroupCurrency 
THEN (COALESCE(PCB.ComponentCostPrice, 0))  
ELSE CAST(COALESCE(PCB.ComponentCostPrice, 0) AS decimal(20,10)) * CAST(GC_Budget.ExchangeRate AS decimal(10,7)) 
END, 0) AS ComponentCostPriceGC_Budget

FROM DataStore5.ProductCostBreakdownTheoretical PCB

--Join to retrieve the name of the resource when it is an item
LEFT JOIN DataStore.Product P
ON PCB.P.CompanyCode
	and PCB.P.ProductCode

--Join to retrieve currency information:
JOIN (SELECT DISTINCT LES.ReportingCurrency
					, LES.AccountingCurrency
					, LES.ExchangeRateType
					, LES.BudgetExchangeRateType
					, LES.`Name`
					, G.GroupCurrencyCode
			FROM dbo.SMRBILedgerStaging LES
			CROSS JOIN (SELECT TOP 1 /*FIXME*/ GroupCurrencyCode FROM ETL.GroupCurrency) G
		) L
ON PCB.L.Name

LEFT JOIN DataStore.ExchangeRate RC -- ReportingCurrency
ON RC.L.AccountingCurrency
	and RC.L.ReportingCurrency
	and RC.L.ExchangeRateType
	and PCB.CalculationDate BETWEEN RC.ValidFrom AND RC.ValidTo

LEFT JOIN DataStore.ExchangeRate GC -- GroupCurrency
ON GC.L.AccountingCurrency
	and GC.L.GroupCurrency
	and GC.L.ExchangeRateType
	and PCB.CalculationDate BETWEEN GC.ValidFrom AND GC.ValidTo

LEFT JOIN DataStore.ExchangeRate RC_Budget -- ReportingCurrency
ON RC_Budget.L.AccountingCurrency
	and RC_Budget.L.ReportingCurrency
	and RC_Budget.L.BudgetExchangeRateType
	and PCB.CalculationDate BETWEEN RC_Budget.ValidFrom AND RC_Budget.ValidTo

LEFT JOIN DataStore.ExchangeRate GC_Budget -- GroupCurrency
ON GC_Budget.L.AccountingCurrency
	and GC_Budget.L.GroupCurrency
	and GC_Budget.L.BudgetExchangeRateType
	and PCB.CalculationDate BETWEEN GC_Budget.ValidFrom AND GC_Budget.ValidTo

--Join to retrieve the different layers:
LEFT JOIN dbo.CostSheetNodeHierarchyTable CSNHT1
ON PCB.CSNHT1.DataAreaId
	and CSNHT1.PCB.CostGroupCode
	and CSNHT1.PCB.Resource_
--Avoid duplication by taking distinct. This does not cover all cases however (e.g. FREIGHT, RM99), so an additional filter is required on the Level 2 Code
LEFT JOIN 
	(SELECT DISTINCT DataAreaId
			, Level_1_Code
			, Level_1_Description
			, Level_1_CostGroupId
			, Level_2_Code
			, Level_2_Description
			, Level_2_CostGroupId
			, Level_3_Code
			, Level_3_Description
			, Level_3_CostGroupId
			--, Level_4_Code
			--, Level_4_Description
			--, Level_4_CostGroupId
			--, Level_5_Code
			--, Level_5_Description
			--, Level_5_CostGroupId
			--, Level_6_Code = CASE WHEN Level_6_Code like 'M%' THEN '' ELSE Level_6_Code END
			--, Level_6_Description = CASE WHEN Level_6_Code like 'M%' THEN '' ELSE Level_6_Description END
			--, Level_6_CostGroupId = CASE WHEN Level_6_Code like 'M%' THEN '' ELSE Level_6_CostGroupId END
		FROM dbo.CostSheetNodeHierarchyTable
		WHERE 1
			and 'Cost of goods manufactured' --Only take into account manufacturing costs
		) CSNHT2
ON PCB.CompanyCode = CSNHT2.DataAreaId
	and PCB.CostGroupCode = CSNHT2.Level_3_CostGroupId
	and PCB.CostPriceCalculationNumber != 'No Calculation'

--Avoid duplication by taking distinct. This does not cover all cases however (e.g. FREIGHT, RM99), so an additional filter is required on the Level 2 Code
LEFT JOIN 
	(SELECT DISTINCT DataAreaId
			, Level_1_Code
			, Level_1_Description
			, Level_1_CostGroupId
			, Level_2_Code
			, Level_2_Description
			, Level_2_CostGroupId
			, Level_3_Code
			, Level_3_Description
			, Level_3_CostGroupId
			--, Level_4_Code
			--, Level_4_Description
			--, Level_4_CostGroupId
			--, Level_5_Code
			--, Level_5_Description
			--, Level_5_CostGroupId
			--, Level_6_Code = CASE WHEN Level_6_Code like 'M%' THEN '' ELSE Level_6_Code END
			--, Level_6_Description = CASE WHEN Level_6_Code like 'M%' THEN '' ELSE Level_6_Description END
			--, Level_6_CostGroupId = CASE WHEN Level_6_Code like 'M%' THEN '' ELSE Level_6_CostGroupId END
		FROM dbo.CostSheetNodeHierarchyTable
		
WHERE 1=1
			and Level_2_Code = 'Costs of purchase' --Only take into account manufacturing costs
		) CSNHT3
ON PCB.CompanyCode = CSNHT3.DataAreaId
	and PCB.CostGroupCode = CSNHT3.Level_3_CostGroupId
	and PCB.CostPriceCalculationNumber = 'No Calculation'
;
);
