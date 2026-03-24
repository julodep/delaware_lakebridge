/****** Object:  View [DWH].[V_FactAccountsPayable]    Script Date: 03/03/2026 16:26:09 ******/











CREATE OR REPLACE VIEW `DWH`.`V_FactAccountsPayable` AS 


SELECT	  RecId
		, UPPER(PurchaseInvoiceCode) AS PurchaseInvoiceCode
		, UPPER(CompanyCode) AS CompanyCode
		, DimIsOpenAmountId
		, UPPER(OutstandingPeriodCode) AS OutstandingPeriodCode
		, UPPER(SupplierCode) AS SupplierCode
		, UPPER(TransactionCurrencyCode) AS TransactionCurrencyCode
		, UPPER(AccountingCurrencyCode) AS AccountingCurrencyCode
		, UPPER(ReportingCurrencyCode) AS ReportingCurrencyCode
		, UPPER(GroupCurrencyCode) AS GroupCurrencyCode
		--, DefaultExchangeRateTypeCode
		--, BudgetExchangeRateTypeCode
		, InvoiceDate
		, ETL.fn_DateKeyInt(DueDate) AS DimDueDateId
		, LastPaymentDate
		, DocumentDate
		--, ETL.fn_DateKeyInt(DimCreationDateId) AS DimCreationDateId
		, ReportDate AS DimReportDateId
		, CAST(LEFT(ReportDate, 6) AS int) AS ReportMonthId
		, PayablesVoucher
		, `Description`
		, InvoiceAmountTC
		, InvoiceAmountAC
		, InvoiceAmountRC
		, InvoiceAmountGC
		, InvoiceAmountAC_Budget
		, InvoiceAmountRC_Budget
		, InvoiceAmountGC_Budget
		, PaidAmountTC
		, PaidAmountAC
		, PaidAmountRC
		, PaidAmountGC
		, PaidAmountAC_Budget
		, PaidAmountRC_Budget
		, PaidAmountGC_Budget
		, OpenAmountTC
		, OpenAmountAC
		, OpenAmountRC
		, OpenAmountGC
		, OpenAmountAC_Budget
		, OpenAmountRC_Budget
		, OpenAmountGC_Budget
		, AppliedExchangeRateTC
		, AppliedExchangeRateRC
		, AppliedExchangeRateAC
		, AppliedExchangeRateGC
		, AppliedExchangeRateRC_Budget
		, AppliedExchangeRateAC_Budget
		, AppliedExchangeRateGC_Budget

FROM DataStore.AccountsPayable
;
