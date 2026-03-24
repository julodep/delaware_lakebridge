/****** Object:  View [DataStore2].[V_ProductionTimeRegistration]    Script Date: 03/03/2026 16:26:09 ******/







CREATE OR REPLACE VIEW `DataStore2`.`V_ProductionTimeRegistration` AS


SELECT PTR.ProductionOrderCode AS ProductionOrderCode
	 , PTR.CompanyCode AS CompanyCode
	 , PTR.ProductConfigurationCode AS ProductConfigurationCode
	 , PTR.ResourceCode AS ResourceCode
	 , PTR.RouteCode AS RouteCode
	 , PTR.OperationCode AS OperationCode
	 , PTR.OperationNumber AS OperationNumber
	 , PTR.RoutingName AS RoutingName
	 , PTR.Shift AS Shift
	 , PTR.OperatorType AS OperatorType
	 , CASE WHEN R.RouteGroupCode = 'CostOnly' THEN UPPER(HCM.Name) ELSE 'N/A' END AS OperatorName
	 , COALESCE(NULLIF(L.ExchangeRateType,''), '_N/A') AS DefaultExchangeRateTypeCode
	 , COALESCE(NULLIF(L.BudgetExchangeRateType,''), '_N/A') AS BudgetExchangeRateTypeCode
	 , COALESCE(NULLIF(L.AccountingCurrency,''), '_N/A') AS TransactionCurrencyCode
	 , COALESCE(NULLIF(L.AccountingCurrency,''), '_N/A') AS AccountingCurrencyCode
	 , COALESCE(NULLIF(L.ReportingCurrency,''), '_N/A') AS ReportingCurrencyCode
	 , COALESCE(NULLIF(L.GroupCurrency,''), '_N/A') AS GroupCurrencyCode
	 , PTR.RecId AS RecId
	   
	   /*Dates*/
	 , PTR.PostedJournalDate AS PostedJournalDate
	  
	   /*Key figures*/
	 , COALESCE(PTR1.Hours*60, 0) AS MachineTimeMinutes
	 , COALESCE(PTR1.Hours, 0) AS MachineTimeHours
	 , COALESCE(PTR1.Hours/24, 0) AS MachineTimeDays
	 , COALESCE(PTR1.Hours, 0) * COALESCE(PTR1.HourPrice, 0) AS MachineCostTC
	 , COALESCE(CASE WHEN L.AccountingCurrency = L.AccountingCurrency THEN COALESCE(PTR1.Hours, 0) * COALESCE(PTR1.HourPrice, 0) 
ELSE COALESCE(PTR1.Hours, 0) * COALESCE(PTR1.HourPrice, 0) * AC.ExchangeRate END, 0) AS MachineCostAC
	 , COALESCE(CASE WHEN L.AccountingCurrency = L.ReportingCurrency THEN COALESCE(PTR1.Hours, 0) * COALESCE(PTR1.HourPrice, 0) 
ELSE COALESCE(PTR1.Hours, 0) * COALESCE(PTR1.HourPrice, 0) * RC.ExchangeRate END, 0) AS MachineCostRC
	 , COALESCE(CASE WHEN L.AccountingCurrency = L.GroupCurrency THEN COALESCE(PTR1.Hours, 0) * COALESCE(PTR1.HourPrice, 0) 
ELSE COALESCE(PTR1.Hours, 0) * COALESCE(PTR1.HourPrice, 0) * GC.ExchangeRate END, 0) AS MachineCostGC
	 , COALESCE(CASE WHEN L.AccountingCurrency = L.AccountingCurrency THEN COALESCE(PTR1.Hours, 0) * COALESCE(PTR1.HourPrice, 0) 
ELSE COALESCE(PTR1.Hours, 0) * COALESCE(PTR1.HourPrice, 0) * AC_Budget.ExchangeRate END, 0) AS MachineCostAC_Budget
	 , COALESCE(CASE WHEN L.AccountingCurrency = L.ReportingCurrency THEN COALESCE(PTR1.Hours, 0) * COALESCE(PTR1.HourPrice, 0) 
ELSE COALESCE(PTR1.Hours, 0) * COALESCE(PTR1.HourPrice, 0) * RC_Budget.ExchangeRate END, 0) AS MachineCostRC_Budget
	 , COALESCE(CASE WHEN L.AccountingCurrency = L.GroupCurrency THEN COALESCE(PTR1.Hours, 0) * COALESCE(PTR1.HourPrice, 0) 
ELSE COALESCE(PTR1.Hours, 0) * COALESCE(PTR1.HourPrice, 0) * GC_Budget.ExchangeRate END, 0) AS MachineCostGC_Budget
	 , COALESCE(PTR2.Hours*60, 0) AS OperatorTimeMinutes
	 , COALESCE(PTR2.Hours, 0) AS OperatorTimeHours
	 , COALESCE(PTR2.Hours/24, 0) AS OperatorTimeDays
	 , COALESCE(PTR2.Hours, 0) * COALESCE(PTR2.HourPrice, 0) AS LabourCostTC
	 , COALESCE(CASE WHEN L.AccountingCurrency = L.AccountingCurrency THEN COALESCE(PTR2.Hours, 0) * COALESCE(PTR2.HourPrice, 0) 
ELSE COALESCE(PTR2.Hours, 0) * COALESCE(PTR2.HourPrice, 0) * AC.ExchangeRate END, 0) AS LabourCostAC
	 , COALESCE(CASE WHEN L.AccountingCurrency = L.ReportingCurrency THEN COALESCE(PTR2.Hours, 0) * COALESCE(PTR2.HourPrice, 0) 
ELSE COALESCE(PTR2.Hours, 0) * COALESCE(PTR2.HourPrice, 0) * RC.ExchangeRate END, 0) AS LabourCostRC
	 , COALESCE(CASE WHEN L.AccountingCurrency = L.GroupCurrency THEN COALESCE(PTR2.Hours, 0) * COALESCE(PTR2.HourPrice, 0) 
ELSE COALESCE(PTR2.Hours, 0) * COALESCE(PTR2.HourPrice, 0) * GC.ExchangeRate END, 0) AS LabourCostGC
	 , COALESCE(CASE WHEN L.AccountingCurrency = L.AccountingCurrency THEN COALESCE(PTR2.Hours, 0) * COALESCE(PTR2.HourPrice, 0) 
ELSE COALESCE(PTR2.Hours, 0) * COALESCE(PTR2.HourPrice, 0) * AC_Budget.ExchangeRate END, 0)	AS LabourCostAC_Budget
	 , COALESCE(CASE WHEN L.AccountingCurrency = L.ReportingCurrency THEN COALESCE(PTR2.Hours, 0) * COALESCE(PTR2.HourPrice, 0) 
ELSE COALESCE(PTR2.Hours, 0) * COALESCE(PTR2.HourPrice, 0) * RC_Budget.ExchangeRate END, 0) AS LabourCostRC_Budget
	 , COALESCE(CASE WHEN L.AccountingCurrency = L.GroupCurrency THEN COALESCE(PTR2.Hours, 0) * COALESCE(PTR2.HourPrice, 0) 
ELSE COALESCE(PTR2.Hours, 0) * COALESCE(PTR2.HourPrice, 0) * GC_Budget.ExchangeRate END, 0) AS LabourCostGC_Budget
	 , CAST(1 AS DECIMAL(38,6)) AS AppliedExchangeRateTC
	 , COALESCE(RC.ExchangeRate, 0) AS  AppliedExchangeRateRC
	 , COALESCE(AC.ExchangeRate, 0) AS  AppliedExchangeRateAC
	 , COALESCE(GC.ExchangeRate, 0) AS  AppliedExchangeRateGC
	 , COALESCE(RC_Budget.ExchangeRate, 0) AS AppliedExchangeRateRC_Budget
	 , COALESCE(AC_Budget.ExchangeRate, 0) AS AppliedExchangeRateAC_Budget
	 , COALESCE(GC_Budget.ExchangeRate, 0) AS AppliedExchangeRateGC_Budget

FROM DataStore.ProductionTimeRegistration PTR
LEFT JOIN DataStore.ProductConfiguration PC
ON PC.CompanyCode = PTR.CompanyCode
AND PC.InventDimCode = PTR.ProductConfigurationCode

LEFT JOIN DataStore.Route R
ON R.CompanyCode = PTR.CompanyCode
AND R.RouteCode = PTR.RouteCode
AND R.OperationCode = PTR.OperationCode
AND R.OperationNumber = PTR.OperationNumber
AND R.SiteCode = PC.SiteCode

LEFT JOIN DataStore.ProductionTimeRegistration PTR1
ON PTR.RecId = PTR1.RecId
AND R.RouteGroupCode <> 'CostOnly'

LEFT JOIN DataStore.ProductionTimeRegistration PTR2
ON PTR.RecId = PTR2.RecId
AND R.RouteGroupCode = 'CostOnly'

LEFT JOIN dbo.SMRBIHcmWorkerStaging HCM
ON HCM.HcmWorkerRecId = PTR.OperatorName

JOIN 
	(SELECT DISTINCT LES.ReportingCurrency
			       , LES.AccountingCurrency
			       , LES.ExchangeRateType
			       , LES.BudgetExchangeRateType
			       , LES.`Name`
			       , GroupCurrency = G.GroupCurrencyCode
	 FROM dbo.SMRBILedgerStaging LES
	 CROSS JOIN (SELECT TOP 1 /*FIXME*/ GroupCurrencyCode FROM ETL.GroupCurrency) G
	) L
ON PTR.CompanyCode = L.Name

--Required for the Actual exchange rates:
LEFT JOIN DataStore.ExchangeRate RC -- ReportingCurrenycy
ON RC.FromCurrencyCode = L.AccountingCurrency
AND RC.ToCurrencyCode = L.ReportingCurrency
AND RC.ExchangeRateTypeCode = L.ExchangeRateType
AND PTR.PostedJournalDate BETWEEN RC.ValidFrom AND RC.ValidTo

LEFT JOIN DataStore.ExchangeRate AC -- AccountingCurrency
ON AC.FromCurrencyCode = L.AccountingCurrency
AND AC.ToCurrencyCode = L.AccountingCurrency
AND AC.ExchangeRateTypeCode = L.ExchangeRateType
AND PTR.PostedJournalDate BETWEEN AC.ValidFrom AND AC.ValidTo

LEFT JOIN DataStore.ExchangeRate GC -- GroupCurrency
ON GC.FromCurrencyCode = L.AccountingCurrency
AND GC.ToCurrencyCode = L.GroupCurrency
AND GC.ExchangeRateTypeCode = L.ExchangeRateType
AND PTR.PostedJournalDate BETWEEN GC.ValidFrom AND GC.ValidTo

--Required for the Budget exchange rates:

LEFT JOIN DataStore.ExchangeRate RC_Budget -- ReportingCurrenycy
ON RC_Budget.FromCurrencyCode = L.AccountingCurrency
AND RC_Budget.ToCurrencyCode = L.ReportingCurrency
AND RC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
AND PTR.PostedJournalDate BETWEEN RC_Budget.ValidFrom AND RC_Budget.ValidTo

LEFT JOIN DataStore.ExchangeRate AC_Budget -- AccountingCurrency
ON AC_Budget.FromCurrencyCode = L.AccountingCurrency
AND AC_Budget.ToCurrencyCode = L.AccountingCurrency
AND AC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
AND PTR.PostedJournalDate BETWEEN AC_Budget.ValidFrom AND AC_Budget.ValidTo

LEFT JOIN DataStore.ExchangeRate GC_Budget -- GroupCurrency
ON GC_Budget.FromCurrencyCode = L.AccountingCurrency  
AND GC_Budget.ToCurrencyCode = L.GroupCurrency
AND GC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
AND PTR.PostedJournalDate BETWEEN GC_Budget.ValidFrom AND GC_Budget.ValidTo
;
