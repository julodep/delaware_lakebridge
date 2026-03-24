/****** Object:  View [DataStore].[V_GeneralLedgerHistoric]    Script Date: 03/03/2026 16:26:08 ******/




--EXECUTE ETL.SPR_LoadDataStoreData 'DataStore', 'GeneralLedgerHistoric', 0

/****** Script for SelectTopNRows command from SSMS  ******/
CREATE OR REPLACE VIEW `DataStore`.`V_GeneralLedgerHistoric`
AS

SELECT CAST(- 1 AS INT) AS RecId
	, CAST('_N/A' AS STRING) AS TransactionCode
	, CAST(UPPER(COALESCE(CompanyCode, '_N/A')) AS STRING) AS CompanyCode
	, CAST(UPPER(L.ExchangeRateType) AS STRING) AS DefaultExchangeRateTypeCode
	, CAST(UPPER(L.BudgetExchangeRateType) AS STRING) AS BudgetExchangeRateTypeCode
	, CAST(UPPER(LES.AccountingCurrency) AS STRING) AS TransactionCurrencyCode
	--CAST(, ISNULL(CAST(UPPER(LES.AccountingCurrency) AS NVARCHAR(5)), N'_N/A') AS AccountingCurrencyCode
	, CAST(COALESCE(UPPER(GL.AccountingCurrencyCode), '_N/A') AS STRING) AS AccountingCurrencyCode
	, COALESCE(CAST(UPPER(LES.ReportingCurrency) AS STRING), '_N/A') AS ReportingCurrencyCode
	, COALESCE(CAST(UPPER(L.GroupCurrency) AS STRING), '_N/A') AS GroupCurrencyCode
	, CAST(UPPER(COALESCE(GLAccountCode, '_N/A')) AS STRING) AS GLAccountCode
	, CAST(UPPER(COALESCE(IntercompanyCode, '_N/A'))  AS STRING) AS InterCompanyCode
	, CAST(UPPER(COALESCE(BusinessSegmentCode, '_N/A'))  AS STRING) AS BusinessSegmentCode
	, CAST(UPPER(COALESCE(DepartmentCode, '_N/A'))  AS STRING) AS DepartmentCode
	, CAST(UPPER(COALESCE(EndCustomerCode, '_N/A')) AS STRING)  AS EndCustomerCode
	, CAST(UPPER(COALESCE(LocationCode, '_N/A')) AS STRING) AS LocationCode
	, CAST(UPPER(COALESCE(ShipmentContractCode, '_N/A'))  AS STRING) AS ShipmentContractCode
	, CAST(UPPER(COALESCE(LocalAccountCode, '_N/A'))  AS STRING) AS LocalAccountCode
	, CAST(UPPER(COALESCE(ProductCode, '_N/A'))  AS STRING) AS ProductFDCode
	, CAST('1900-01-01' AS timestamp) AS DocumentDate
	, CAST(DimPostingDateId as int) AS DimPostingDateId
	, '_N/A' AS Voucher
	, COALESCE(CASE 
WHEN GL.AccountingCurrencyCode = LES.AccountingCurrency
THEN (COALESCE(REPLACE(amountAC, ',', '.'), 0))
ELSE (COALESCE(REPLACE(amountAC, ',', '.'), 0)) * TC.ExchangeRate
END, 0) AS AmountTC
	, CAST(COALESCE(REPLACE(amountAC, ',', '.'), 0) as DECIMAL(38,6)) AS AmountAC
	, CAST(COALESCE(REPLACE(amountAC, ',', '.'), 0) as DECIMAL(38,6))  AS AmountRC
	, COALESCE(CASE 
WHEN GL.AccountingCurrencyCode = L.GroupCurrency
THEN (COALESCE(REPLACE(amountAC, ',', '.'), 0))
ELSE (COALESCE(REPLACE(amountAC, ',', '.'), 0)) * GC.ExchangeRate
END, 0) AS AmountGC
	, COALESCE(TC.ExchangeRate, 1) AS AppliedExchangeRateTC
	, CAST(1 AS DECIMAL(38,17)) AS AppliedExchangeRateAC
	, CAST(1 AS DECIMAL(38,17)) AS AppliedExchangeRateRC
	, COALESCE(GC.ExchangeRate, 1) AS AppliedExchangeRateGC
FROM StagingHistoric.GeneralLedger GL
INNER JOIN (
	SELECT DISTINCT *
	FROM dbo.SMRBILedgerStaging
	) LES
	ON GL.CompanyCode = LES.Name
INNER JOIN (
	SELECT DISTINCT LES.ReportingCurrency
		, LES.AccountingCurrency
		, LES.ExchangeRateType
		, LES.BudgetExchangeRateType
		, LES.`Name`
		, GroupCurrency = G.GroupCurrencyCode
	FROM dbo.SMRBILedgerStaging LES
	CROSS JOIN (
		SELECT TOP 1 /*FIXME*/ GroupCurrencyCode
		FROM ETL.GroupCurrency
		) G
	) L
	ON L.`Name` = GL.CompanyCode
LEFT JOIN DataStore.ExchangeRate GC -- GroupCurrency
	ON GC.FromCurrencyCode = GL.AccountingCurrencyCode --LES.AccountingCurrency
		AND GC.ToCurrencyCode = L.GroupCurrency AND GC.ExchangeRateTypeCode = L.ExchangeRateType AND GL.DimPostingDateId BETWEEN GC.ValidFrom AND GC.ValidTo
LEFT JOIN DataStore.ExchangeRate TC -- GroupCurrency
	ON TC.FromCurrencyCode = GL.AccountingCurrencyCode --LES.AccountingCurrency
		AND TC.ToCurrencyCode = LES.AccountingCurrency AND TC.ExchangeRateTypeCode = L.ExchangeRateType AND GL.DimPostingDateId BETWEEN TC.ValidFrom AND TC.ValidTo
;
