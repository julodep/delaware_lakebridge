/****** Object:  View [DataStore2].[V_GeneralLedgerBudget]    Script Date: 03/03/2026 16:26:09 ******/


















CREATE OR REPLACE VIEW `DataStore2`.`V_GeneralLedgerBudget` AS 


SELECT CONCAT(BREHS.TransactionNumber,UPPER(BREHS.Company),COALESCE(BRELS.EntryDate, '1900-01-01'),UPPER(BREHS.BudgetModelId)) AS GeneralBudgetIdScreening
	
	/* Details */
		 , BREHS.TransactionNumber AS TransactionNumber
		 , UPPER(BREHS.BudgetModelId) AS BudgetModelCode
		 , UPPER(BREHS.BudgetSubModelId) AS BudgetSubModelCode
		 , UPPER(BREHS.Company) AS CompanyCode
		 , UPPER(BREHS.BudgetCode) AS BudgetTransactionCode
		 , COALESCE(SM.Name, '_N/A') AS BudgetType
	
	/* Dates */
		 , COALESCE(BRELS.EntryDate, '1900-01-01') AS BudgetDate --Determines the period that the budget will be allocated	 
	
	/* Financial Dimensions */
		/* ALTER/ADD additional dimensions if required */
		--, ISNULL(ADL.MainAccount,-1) AS GLAccountId /* !!!KEEP!!! */
		, COALESCE(GLA.GLAccountId, -1) AS GLAccountId
		, COALESCE(ADL.Intercompany, -1) AS IntercompanyId  
		--, ISNULL(ADL.CostCenter, -1) AS CostCenterId
		, COALESCE(SEG.BusinessSegmentId, -1) AS BusinessSegmentId  
		, COALESCE(DEP.DepartmentId, -1) AS DepartmentId  
		, COALESCE(ADL.EndCustomer, -1) AS EndCustomerId  
		, COALESCE(LOC.LocationId, -1) AS LocationId  
		, COALESCE(ADL.ShipmentContract, -1) AS ShipmentContractId  
		, COALESCE(ADL.LocalAccount, -1) AS LocalAccountId  
		, COALESCE(ADL.Product, -1) AS ProductFDId  
	
	/* Dimensions */	 
		 , L.ExchangeRateType AS DefaultExchangeRateTypeCode
		 , L.BudgetExchangeRateType AS BudgetExchangeRateTypeCode
		 , UPPER(BRELS.CurrencyCode) AS TransactionCurrencyCode
		 , COALESCE(CAST(UPPER(L.AccountingCurrency) AS STRING), '_N/A') AS AccountingCurrencyCode
		 , COALESCE(CAST(UPPER(L.ReportingCurrency) AS STRING), '_N/A') AS ReportingCurrencyCode
		 , COALESCE(CAST(UPPER(L.GroupCurrency) AS STRING), '_N/A') AS GroupCurrencyCode
	
	/* Measures */
		 , COALESCE(CASE WHEN BRELS.AmountType = 1 THEN BRELS.TransactionCurrencyAmount * -1 ELSE BRELS.TransactionCurrencyAmount END, 0) AS BudgetAmountTC
		 , COALESCE(CASE WHEN BRELS.CurrencyCode = L.AccountingCurrency 
THEN (COALESCE(CASE WHEN BRELS.AmountType = 1 THEN BRELS.TransactionCurrencyAmount * -1 ELSE BRELS.TransactionCurrencyAmount END, 0)) 
ELSE (COALESCE(CASE WHEN BRELS.AmountType = 1 THEN BRELS.TransactionCurrencyAmount * -1 ELSE BRELS.TransactionCurrencyAmount END, 0)) 
* AC.ExchangeRate END, 0) AS BudgetAmountAC
		 , COALESCE(CASE WHEN BRELS.CurrencyCode = L.ReportingCurrency 
THEN (COALESCE(CASE WHEN BRELS.AmountType = 1 THEN BRELS.TransactionCurrencyAmount * -1 ELSE BRELS.TransactionCurrencyAmount END, 0))  
ELSE (COALESCE(CASE WHEN BRELS.AmountType = 1 THEN BRELS.TransactionCurrencyAmount * -1 ELSE BRELS.TransactionCurrencyAmount END, 0)) 
* RC.ExchangeRate END, 0) AS BudgetAmountRC
		 , COALESCE(CASE WHEN BRELS.CurrencyCode = L.GroupCurrency 
THEN (COALESCE(CASE WHEN BRELS.AmountType = 1 THEN BRELS.TransactionCurrencyAmount * -1 ELSE BRELS.TransactionCurrencyAmount END, 0))  
ELSE (COALESCE(CASE WHEN BRELS.AmountType = 1 THEN BRELS.TransactionCurrencyAmount * -1 ELSE BRELS.TransactionCurrencyAmount END, 0)) 
* GC.ExchangeRate END, 0) AS BudgetAmountGC	 
		 , COALESCE(CASE WHEN BRELS.CurrencyCode = L.AccountingCurrency 
THEN (COALESCE(CASE WHEN BRELS.AmountType = 1 THEN BRELS.TransactionCurrencyAmount * -1 ELSE BRELS.TransactionCurrencyAmount END, 0))  
ELSE (COALESCE(CASE WHEN BRELS.AmountType = 1 THEN BRELS.TransactionCurrencyAmount * -1 ELSE BRELS.TransactionCurrencyAmount END, 0)) 
* AC_Budget.ExchangeRate END, 0) AS BudgetAmountAC_Budget
		 , COALESCE(CASE WHEN BRELS.CurrencyCode = L.ReportingCurrency 
THEN (COALESCE(CASE WHEN BRELS.AmountType = 1 THEN BRELS.TransactionCurrencyAmount * -1 ELSE BRELS.TransactionCurrencyAmount END, 0))  
ELSE (COALESCE(CASE WHEN BRELS.AmountType = 1 THEN BRELS.TransactionCurrencyAmount * -1 ELSE BRELS.TransactionCurrencyAmount END, 0)) 
* RC_Budget.ExchangeRate END, 0) AS BudgetAmountRC_Budget
		 , COALESCE(CASE WHEN BRELS.CurrencyCode = L.GroupCurrency 
THEN (COALESCE(CASE WHEN BRELS.AmountType = 1 THEN BRELS.TransactionCurrencyAmount * -1 ELSE BRELS.TransactionCurrencyAmount END, 0))  
ELSE (COALESCE(CASE WHEN BRELS.AmountType = 1 THEN BRELS.TransactionCurrencyAmount * -1 ELSE BRELS.TransactionCurrencyAmount END, 0)) 
* GC_Budget.ExchangeRate END, 0) AS BudgetAmountGC_Budget
		 , CAST(1 AS DECIMAL(38,6)) AS AppliedExchangeRateTC
		 , COALESCE(AC.ExchangeRate, 0) AS AppliedExchangeRateAC
		 , COALESCE(RC.ExchangeRate, 0) AS AppliedExchangeRateRC
		 , COALESCE(GC.ExchangeRate, 0) AS AppliedExchangeRateGC
		 , COALESCE(AC_Budget.ExchangeRate, 0) AS AppliedExchangeRateAC_Budget
		 , COALESCE(RC_Budget.ExchangeRate, 0) AS AppliedExchangeRateRC_Budget
		 , COALESCE(GC_Budget.ExchangeRate, 0) AS AppliedExchangeRateGC_Budget       
		 
FROM dbo.SMRBIBudgetRegisterEntryHeaderStaging BREHS

INNER JOIN dbo.SMRBIBudgetRegisterEntryLineStaging BRELS
ON BREHS.BudgetRegisterRecId = BRELS.BudgetTransactionHeaderRecId
-------------------
--LedgerDimensionDisplayValye--GLAccountCode (MainAccount) | BusinessSegmentCode | DepartmentCode | LocationCode
-------------------
--20210401 Link with GLAccount
LEFT JOIN DataStore.GLAccount GLA
ON GLA.CompanyCode = BRELS.LEGALENTITYID
--AND GLA.GLAccountCode = SUBSTRING(BRELS.ledgerdimensiondisplayvalue, 0, CHARINDEX('|', BRELS.ledgerdimensiondisplayvalue))
AND GLA.GLAccountCode = dbo.Cust_NTH_Element(LEDGERDIMENSIONDISPLAYVALUE, '|', 0)

--20220329 link with businesssegment
LEFT JOIN DataStore.BusinessSegment SEG
ON SEG.BusinessSegmentCode = dbo.Cust_NTH_ELEMENT(LEDGERDIMENSIONDISPLAYVALUE, '|', 1)

--20220329 link with department
LEFT JOIN DataStore.Department DEP
ON DEP.DepartmentCode = dbo.Cust_NTH_ELEMENT(LEDGERDIMENSIONDISPLAYVALUE, '|', 2)

--20220329 link with location
LEFT JOIN DataStore.Location LOC
ON LOC.LocationCode = dbo.Cust_NTH_ELEMENT(LEDGERDIMENSIONDISPLAYVALUE, '|', 3)



LEFT JOIN DataStore.AnalyticalDimensionLedger ADL
ON ADL.LedgerDimensionId = -1 -- to be adapted with correct ledger dimension


INNER JOIN 
	(SELECT DISTINCT LES.ReportingCurrency
			       , LES.AccountingCurrency
			       , LES.ExchangeRateType
			       , LES.BudgetExchangeRateType
			       , LES.`Name`
			       , G.GroupCurrencyCode AS GroupCurrency
	 FROM dbo.SMRBILedgerStaging LES
	 CROSS JOIN (SELECT TOP 1 /*FIXME*/ GroupCurrencyCode FROM ETL.GroupCurrency) G
	) L
ON L.`Name` = BREHS.Company

LEFT JOIN ETL.StringMap SM
ON SM.SourceTable = 'BudgetTransactionLine'
AND SM.Enum = CAST(AmountType AS STRING)

LEFT JOIN DataStore.ExchangeRate RC -- ReportingCurrenycy
ON RC.FromCurrencyCode = BRELS.CurrencyCode
AND RC.ToCurrencyCode = L.ReportingCurrency
AND RC.ExchangeRateTypeCode = L.ExchangeRateType
AND BRELS.EntryDate BETWEEN RC.ValidFrom AND RC.ValidTo

LEFT JOIN DataStore.ExchangeRate AC -- AccountingCurrency
ON AC.FromCurrencyCode = BRELS.CurrencyCode
AND AC.ToCurrencyCode = L.AccountingCurrency
AND AC.ExchangeRateTypeCode = L.ExchangeRateType
AND BRELS.EntryDate BETWEEN AC.ValidFrom AND AC.ValidTo

LEFT JOIN DataStore.ExchangeRate GC -- GroupCurrency
ON GC.FromCurrencyCode = BRELS.CurrencyCode
AND GC.ToCurrencyCode = L.GroupCurrency
AND GC.ExchangeRateTypeCode = L.ExchangeRateType
AND BRELS.EntryDate BETWEEN GC.ValidFrom AND GC.ValidTo

LEFT JOIN DataStore.ExchangeRate RC_Budget -- ReportingCurreny
ON RC_Budget.FromCurrencyCode = BRELS.CurrencyCode
AND RC_Budget.ToCurrencyCode = L.ReportingCurrency
AND RC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
AND BRELS.EntryDate BETWEEN RC_Budget.ValidFrom AND RC_Budget.ValidTo

LEFT JOIN DataStore.ExchangeRate AC_Budget -- AccountingCurrency
ON AC_Budget.FromCurrencyCode = BRELS.CurrencyCode
AND AC_Budget.ToCurrencyCode = L.AccountingCurrency
AND AC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
AND BRELS.EntryDate BETWEEN AC_Budget.ValidFrom AND AC_Budget.ValidTo

LEFT JOIN DataStore.ExchangeRate GC_Budget -- GroupCurrency
ON GC_Budget.FromCurrencyCode = BRELS.CurrencyCode
AND GC_Budget.ToCurrencyCode = L.GroupCurrency
AND GC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
AND BRELS.EntryDate BETWEEN GC_Budget.ValidFrom AND GC_Budget.ValidTo
;
