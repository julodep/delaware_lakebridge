/****** Object:  View [DataStore2].[V_GeneralLedger]    Script Date: 03/03/2026 16:26:09 ******/















CREATE OR REPLACE VIEW `DataStore2`.`V_GeneralLedger` AS 

/* Adapt the financial dimensions !! */
;
SELECT	CONCAT(GJAES.GeneralJournalAccountEntryRecId, GJAES.LedgerName) AS GeneralLedgerIdScreening
		--,GLA.GLAccountType

	/* Details */
			, GJAES.GeneralJournalAccountEntryRecId AS RecId
			--, ISNULL(CASE WHEN GJAES.[Description] = '' THEN NULL ELSE GJAES.[Description] END, '_N/A') AS TransactionText
			, COALESCE(CASE WHEN GJAES.YSLETEXT = '' THEN NULL ELSE GJAES.YSLETEXT END, '_N/A') AS TransactionText
			, COALESCE(NULLIF(GJAES.DocumentNumber, ''), '_N/A') AS TransactionCode
		, IsDebitCredit = CAST(CASE 
							WHEN GJAES.IsCredit = 1 THEN 'Credit'
							WHEN GJAES.IsCredit = 0 THEN 'Debit'
							ELSE '_N/A' END AS STRING)
		, Voucher = COALESCE(NULLIF(UPPER(GJAES.Voucher), ''), '_N/A')

	/* Dimensions */
		, UPPER(GJAES.LedgerName) AS CompanyCode		
		, L.ExchangeRateType AS DefaultExchangeRateTypeCode
		, L.BudgetExchangeRateType AS BudgetExchangeRateTypeCode	
		, UPPER(GJAES.TRANSACTIONCURRENCYCODE) AS TransactionCurrencyCode
		, COALESCE(CAST(UPPER(LES.AccountingCurrency) AS STRING), N'_N/A') AS AccountingCurrencyCode
		, COALESCE(CAST(UPPER(LES.ReportingCurrency) AS STRING), '_N/A') AS ReportingCurrencyCode
		, COALESCE(CAST(UPPER(L.GroupCurrency) AS STRING), '_N/A') AS GroupCurrencyCode

	/* Financial Dimensions */
		/* ALTER/ADD additional dimensions if required */
		, COALESCE(ADL.MainAccount, -1) AS GLAccountId /* !!!KEEP!!! */
		, COALESCE(ADL.Intercompany, -1) AS IntercompanyId  
		--, ISNULL(ADL.CostCenter, -1) AS CostCenterId
		 , COALESCE(ADL.BusinessSegment, -1) AS BusinessSegmentId
		 , COALESCE(ADL.Department, -1) AS DepartmentId
		, COALESCE(ADL.EndCustomer, -1) AS EndCustomerId
		, COALESCE(ADL.`Location`, -1) AS LocationId
		, COALESCE(ADL.ShipmentContract, -1) AS ShipmentContractId
		, COALESCE(ADL.LocalAccount, -1) AS LocalAccountId
		, COALESCE(ADL.Product, -1) AS ProductFDId
		, COALESCE(ADL.Vendor, -1) AS VendorId

	/* Dates */
	--20221012 Fallback: if no documentdate, use accountingdate
		, COALESCE(NULLIF(GJAES.DocumentDate, '1900-01-01'), GJAES.ACCOUNTINGDATE) AS DocumentDate
		, COALESCE(GJAES.AccountingDate, '1900-01-01') AS PostingDate

	/* Measures */
		, COALESCE(GJAES.TransactionCurrencyAmount, 0) AS AmountTC
		, COALESCE(GJAES.AccountingCurrencyAmount, 0) AS AmountAC
		, COALESCE(GJAES.ReportingCurrencyAmount, 0) AS AmountRC

-- I-369112 : Update of field AmountGC - 20211026
		, COALESCE(GJAES.ReportingCurrencyAmount, 0) AS AmountGC 

		--CASE WHEN GLA.GLAccountType = 'Balance sheet' THEN
		--ISNULL(CASE WHEN L.AccountingCurrency = L.GroupCurrency THEN (ISNULL(GJAES.AccountingCurrencyAmount, 0))  
		--			ELSE (ISNULL(GJAES.AccountingCurrencyAmount, 0)) * GC_Latest.ExchangeRate 
		--		END, 0) 
		--	WHEN GLA.GLAccountType = 'Profit and loss' THEN
		--ISNULL(CASE WHEN L.AccountingCurrency = L.GroupCurrency THEN (ISNULL(GJAES.AccountingCurrencyAmount, 0))  
		--			ELSE (ISNULL(GJAES.AccountingCurrencyAmount, 0)) * GC_ACG.ExchangeRate 
		--		END, 0) 
		--ELSE
		--ISNULL(CASE WHEN L.AccountingCurrency = L.GroupCurrency THEN (ISNULL(GJAES.AccountingCurrencyAmount, 0))  
		--			ELSE (ISNULL(GJAES.AccountingCurrencyAmount, 0)) * GC.ExchangeRate 
		--		END, 0) 
		--END AS AmountGC	  
		, COALESCE(CASE WHEN GJAES.TransactionCurrencyCode = L.AccountingCurrency THEN (COALESCE(GJAES.TransactionCurrencyAmount, 0))  
ELSE (COALESCE(GJAES.TransactionCurrencyAmount, 0)) * AC_Budget.ExchangeRate 
END, 0) AS AmountAC_Budget
		, COALESCE(CASE WHEN GJAES.TransactionCurrencyCode = L.ReportingCurrency THEN (COALESCE(GJAES.TransactionCurrencyAmount, 0))  
ELSE (COALESCE(GJAES.TransactionCurrencyAmount, 0)) * RC_Budget.ExchangeRate 
END, 0) AS AmountRC_Budget
		, COALESCE(CASE WHEN GJAES.TransactionCurrencyCode = L.GroupCurrency THEN (COALESCE(GJAES.TransactionCurrencyAmount, 0))  
ELSE (COALESCE(GJAES.TransactionCurrencyAmount, 0)) * GC_Budget.ExchangeRate 
END, 0) AS AmountGC_Budget	  
		, CAST(1 AS DECIMAL(38,17)) AS AppliedExchangeRateTC
		, COALESCE(GJAES.AccountingCurrencyAmount / COALESCE(NULLIF(GJAES.TransactionCurrencyAmount, 0), 0.000001), 1) AS AppliedExchangeRateAC
		, COALESCE(GJAES.ReportingCurrencyAmount / COALESCE(NULLIF(GJAES.TransactionCurrencyAmount, 0), 0.000001), 1) AS AppliedExchangeRateRC
		, CASE WHEN GLA.GLAccountType = 'Balance sheet' THEN  COALESCE(GC_Latest.ExchangeRate, 1) 
				WHEN GLA.GLAccountType = 'Profit and loss' THEN COALESCE(GC_ACG.ExchangeRate, 1) 
				ELSE COALESCE(GC.ExchangeRate, 1) 
		END AS AppliedExchangeRateGC
		, COALESCE(AC_Budget.ExchangeRate, 1) AS AppliedExchangeRateAC_Budget
		, COALESCE(RC_Budget.ExchangeRate, 1) AS AppliedExchangeRateRC_Budget
		, COALESCE(GC_Budget.ExchangeRate, 1) AS AppliedExchangeRateGC_Budget

FROM dbo.SMRBIGeneralJournalAccountEntryStaging GJAES

INNER JOIN (SELECT DISTINCT * FROM dbo.SMRBILedgerStaging) LES
ON GJAES.LedgerName = LES.Name --Note! Not joining on LedgerRecId but on LedgerName: the data entity GJAES is linked to LES

--Required for Financial Dimensions
LEFT JOIN DataStore.AnalyticalDimensionLedger ADL
ON ADL.LedgerDimensionId = GJAES.GeneralJournalAccountEntryDimension

--20210406 For getting the correct GLAccountType for using correct exchange rates
LEFT JOIN DataStore.GLAccount GLA
ON GLA.CompanyCode = UPPER(GJAES.LedgerName)
AND GLA.GLAccountId = ADL.MainAccount

--Required for the currencies
INNER JOIN 
	(SELECT DISTINCT LES.ReportingCurrency
			       , LES.AccountingCurrency
			       , LES.ExchangeRateType
			       , LES.BudgetExchangeRateType
			       , LES.`Name`
			       , GroupCurrency = G.GroupCurrencyCode
	 FROM dbo.SMRBILedgerStaging LES
	 CROSS JOIN (SELECT TOP 1 /*FIXME*/ GroupCurrencyCode FROM ETL.GroupCurrency) G
	) L
ON L.`Name` = GJAES.LedgerName

INNER JOIN DataStore2.Date DAT
ON DAT.TIMESTAMP = GJAES.ACCOUNTINGDATE

LEFT JOIN DataStore2.ExchangeRateExplosion AC -- AccountingCurrency
ON AC.FromCurrencyCode = GJAES.TransactionCurrencyCode
AND AC.ToCurrencyCode = L.AccountingCurrency
AND AC.ExchangeRateTypeCode = L.ExchangeRateType
AND GJAES.AccountingDate = AC.TIMESTAMP

LEFT JOIN DataStore2.ExchangeRateExplosion RC -- ReportingCurrency
ON RC.FromCurrencyCode = GJAES.TransactionCurrencyCode
AND RC.ToCurrencyCode = L.ReportingCurrency
AND RC.ExchangeRateTypeCode = L.ExchangeRateType
AND GJAES.AccountingDate = RC.TIMESTAMP

LEFT JOIN DataStore2.ExchangeRateExplosion GC -- GroupCurrency
ON GC.FromCurrencyCode = GJAES.TransactionCurrencyCode
AND GC.ToCurrencyCode = L.GroupCurrency
AND GC.ExchangeRateTypeCode = L.ExchangeRateType
AND GJAES.AccountingDate = GC.TIMESTAMP

--20210406
LEFT JOIN (SELECT *
FROM (
	SELECT *
		,ROW_NUMBER() OVER (
			PARTITION BY ExchangeRateTypeCode
			,ExchangeRateTypeName
			,DataSource
			,FromCurrencyCode
			,ToCurrencyCode ORDER BY ValidTo DESC
			) AS rn
	FROM DataStore.ExchangeRate
	) T
WHERE T.rn = 1) GC_Latest -- GroupCurrency Latest
ON GC_Latest.FromCurrencyCode = GJAES.TransactionCurrencyCode
AND GC_Latest.ToCurrencyCode = L.GroupCurrency
AND GC_Latest.ExchangeRateTypeCode = L.ExchangeRateType

LEFT JOIN (SELECT FromCurrencyCode, ToCurrencyCode, ExchangeRateTypeCode, FYYearId, AVG(ExchangeRate) ExchangeRate 
FROM DataStore2.ExchangeRateExplosion EXC
INNER JOIN DataStore2.Date DAT ON DAT.TIMESTAMP = EXC.TIMESTAMP
GROUP BY FromCurrencyCode, ToCurrencyCode, ExchangeRateTypeCode, FYYearId) GC_ACG	--Weighted average on GroupCurrency
ON GC_ACG.FromCurrencyCode = GJAES.TransactionCurrencyCode
AND GC_ACG.ToCurrencyCode = L.AccountingCurrency
AND GC_ACG.ExchangeRateTypeCode = L.BudgetExchangeRateType
AND GC_ACG.FYYearId = DAT.FYYearId

LEFT JOIN DataStore2.ExchangeRateExplosion AC_Budget -- AccountingCurrency
ON AC_Budget.FromCurrencyCode = GJAES.TransactionCurrencyCode
AND AC_Budget.ToCurrencyCode = L.AccountingCurrency
AND AC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
AND GJAES.AccountingDate = AC_Budget.TIMESTAMP

LEFT JOIN DataStore2.ExchangeRateExplosion RC_Budget -- ReportingCurrency
ON RC_Budget.FromCurrencyCode = GJAES.TransactionCurrencyCode
AND RC_Budget.ToCurrencyCode = L.ReportingCurrency
AND RC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
AND GJAES.AccountingDate = RC_Budget.TIMESTAMP

LEFT JOIN DataStore2.ExchangeRateExplosion GC_Budget -- GroupCurrency
ON GC_Budget.FromCurrencyCode = GJAES.TransactionCurrencyCode
AND GC_Budget.ToCurrencyCode = L.GroupCurrency
AND GC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
AND GJAES.AccountingDate = GC_Budget.TIMESTAMP

/* Create unknown member */


UNION ALL

SELECT	'_N/A' AS GeneralLedgerIdScreening
		  , -1 AS RecId
		  , '_N/A' AS TransactionText
		  , '_N/A' AS TransactionCode
		  , 'Debit' AS IsDebitCredit
		  , '_N/A' AS Voucher
		  , UPPER(CompanyId) AS CompanyCode
		  , '_N/A' AS DefaultExchangeRateTypeCode
		  , '_N/A' AS BudgetExchangeRateTypeCode
		  , '_N/A' AS TransactionCurrencyCode
		  , '_N/A' AS AccountingCurrencyCode
		  , '_N/A' AS ReportingCurrencyCode
		  , '_N/A' AS GroupCurrencyCode
		  , -1 AS GLAccountId
		  , -1 AS IntercompanyId
		  --, -1 AS CostCenterId
			, -1 AS BusinessSegmentId
			, -1 AS DepartmentId
			, -1 AS EndCustomerId
			, -1 AS LocationId
			, -1 AS ShipmentContractId
			, -1 AS LocalAccountId
			, -1 AS ProductFDId
			, -1 AS VendorId
		  , '1900-01-01' AS DocumentDate
		  , '1900-01-01' AS PostingDate
		  , 0 AS AmountTC
		  , 0 AS AmountAC
		  , 0 AS AmountRC
		  , 0 AS AmountGC
		  , 0 AS AmountAC_Budget
		  , 0 AS AmountRC_Budget
		  , 0 AS AmountGC_Budget
		  , 1 AS AppliedExchangeRateTC
		  , 1 AS AppliedExchangeRateAC
		  , 1 AS AppliedExchangeRateRC
		  , 1 AS AppliedExchangeRateGC
		  , 1 AS AppliedExchangeRateAC_Budget
		  , 1 AS AppliedExchangeRateRC_Budget
		  , 1 AS AppliedExchangeRateGC_Budget
FROM dbo.SMRBIOfficeAddInLegalEntityStaging


;
