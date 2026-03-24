/****** Object:  View [DataStore2].[V_GeneralLedger_Backup]    Script Date: 03/03/2026 16:26:08 ******/












CREATE OR REPLACE VIEW `DataStore2`.`V_GeneralLedger_Backup` AS 

/* Adapt the financial dimensions !! */
;
SELECT	CONCAT(GJAES.GeneralJournalAccountEntryRecId, GJAES.LedgerName) AS GeneralLedgerIdScreening
	/* Details */
			, GJAES.GeneralJournalAccountEntryRecId AS RecId
			, COALESCE(CASE WHEN GJAES.`Description` = '' THEN NULL ELSE GJAES.`Description` END, '_N/A') AS TransactionText
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
		, COALESCE(GJAES.DocumentDate, '1900-01-01') AS DocumentDate
		, COALESCE(GJAES.AccountingDate, '1900-01-01') AS PostingDate

	/* Measures */
		, COALESCE(GJAES.TransactionCurrencyAmount, 0) AS AmountTC
		, COALESCE(GJAES.AccountingCurrencyAmount, 0) AS AmountAC
		, COALESCE(GJAES.ReportingCurrencyAmount, 0) AS AmountRC
		, COALESCE(CASE WHEN L.AccountingCurrency = L.GroupCurrency THEN (COALESCE(GJAES.AccountingCurrencyAmount, 0))  
ELSE (COALESCE(GJAES.AccountingCurrencyAmount, 0)) * GC.ExchangeRate 
END, 0) AS AmountGC	  
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
		, COALESCE(GC.ExchangeRate, 1) AS AppliedExchangeRateGC
		, COALESCE(AC_Budget.ExchangeRate, 1) AS AppliedExchangeRateAC_Budget
		, COALESCE(RC_Budget.ExchangeRate, 1) AS AppliedExchangeRateRC_Budget
		, COALESCE(GC_Budget.ExchangeRate, 1) AS AppliedExchangeRateGC_Budget

FROM dbo.SMRBIGeneralJournalAccountEntryStaging GJAES

INNER JOIN (SELECT DISTINCT * FROM dbo.SMRBILedgerStaging) LES
ON GJAES.LedgerName = LES.Name --Note! Not joining on LedgerRecId but on LedgerName: the data entity GJAES is linked to LES

--Required for Financial Dimensions
LEFT JOIN DataStore.AnalyticalDimensionLedger ADL
ON ADL.LedgerDimensionId = GJAES.GeneralJournalAccountEntryDimension

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

LEFT JOIN DataStore.ExchangeRate AC -- AccountingCurrency
ON AC.FromCurrencyCode = GJAES.TransactionCurrencyCode
AND AC.ToCurrencyCode = L.AccountingCurrency
AND AC.ExchangeRateTypeCode = L.ExchangeRateType
AND GJAES.AccountingDate BETWEEN AC.ValidFrom AND AC.ValidTo

LEFT JOIN DataStore.ExchangeRate RC -- ReportingCurrency
ON RC.FromCurrencyCode = GJAES.TransactionCurrencyCode
AND RC.ToCurrencyCode = L.ReportingCurrency
AND RC.ExchangeRateTypeCode = L.ExchangeRateType
AND GJAES.AccountingDate BETWEEN RC.ValidFrom AND RC.ValidTo

LEFT JOIN DataStore.ExchangeRate GC -- GroupCurrency
ON GC.FromCurrencyCode = GJAES.TransactionCurrencyCode
AND GC.ToCurrencyCode = L.GroupCurrency
AND GC.ExchangeRateTypeCode = L.ExchangeRateType
AND GJAES.AccountingDate BETWEEN GC.ValidFrom AND GC.ValidTo

LEFT JOIN DataStore.ExchangeRate AC_Budget -- AccountingCurrency
ON AC_Budget.FromCurrencyCode = GJAES.TransactionCurrencyCode
AND AC_Budget.ToCurrencyCode = L.AccountingCurrency
AND AC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
AND GJAES.AccountingDate BETWEEN AC_Budget.ValidFrom AND AC_Budget.ValidTo

LEFT JOIN DataStore.ExchangeRate RC_Budget -- ReportingCurrency
ON RC_Budget.FromCurrencyCode = GJAES.TransactionCurrencyCode
AND RC_Budget.ToCurrencyCode = L.ReportingCurrency
AND RC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
AND GJAES.AccountingDate BETWEEN RC_Budget.ValidFrom AND RC_Budget.ValidTo

LEFT JOIN DataStore.ExchangeRate GC_Budget -- GroupCurrency
ON GC_Budget.FromCurrencyCode = GJAES.TransactionCurrencyCode
AND GC_Budget.ToCurrencyCode = L.GroupCurrency
AND GC_Budget.ExchangeRateTypeCode = L.BudgetExchangeRateType
AND GJAES.AccountingDate BETWEEN GC_Budget.ValidFrom AND GC_Budget.ValidTo

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
