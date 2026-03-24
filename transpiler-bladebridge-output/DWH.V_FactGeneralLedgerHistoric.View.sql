/****** Object:  View [DWH].[V_FactGeneralLedgerHistoric]    Script Date: 03/03/2026 16:26:08 ******/





/****** Script for SelectTopNRows command from SSMS  ******/
CREATE OR REPLACE VIEW `DWH`.`V_FactGeneralLedgerHistoric`
AS

SELECT RecId
	, TransactionCode 
	, '_N/A' AS TransactionText
	, CompanyCode
	, DefaultExchangeRateTypeCode
	, BudgetExchangeRateTypeCode
	, TransactionCurrencyCode
	, AccountingCurrencyCode
	, ReportingCurrencyCode
	, GroupCurrencyCode
	, GLAccountCode  --AS GLAccountId
	, InterCompanyCode -- AS InterCompanyId
	, BusinessSegmentCode -- AS BusinessSegmentId
	, DepartmentCode --AS DepartmentId	
	, EndCustomerCode --AS EndCustomerId
	, LocationCode --AS LocationId
	, ShipmentContractCode --AS ShipmentContractId
	, LocalAccountCode --AS LocalAccountId
	, ProductFDCode --AS ProductFDId
	, '_N/A' AS SupplierCode
	, DocumentDate
	, DimPostingDateId
	, Voucher
	, AmountTC
	, AmountAC
	, AmountRC
	, AmountGC
	, AppliedExchangeRateTC
	, AppliedExchangeRateAC
	, AppliedExchangeRateRC
	, AppliedExchangeRateGC
	, 0 AS AmountAC_Budget
	, 0 AS AmountRC_Budget
	, 0 AS AmountGC_Budget
	, 0 AS AppliedExchangeRateAC_Budget
	, 0 AS AppliedExchangeRateRC_Budget
	, 0 AS AppliedExchangeRateGC_Budget
FROM DataStore.GeneralLedgerHistoric

;
