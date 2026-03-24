/****** Object:  View [DWH].[V_FactAccountsReceivable]    Script Date: 03/03/2026 16:26:09 ******/













CREATE OR REPLACE VIEW `DWH`.`V_FactAccountsReceivable` AS 


SELECT	  RecId
		, UPPER(CompanyCode) AS CompanyCode
		, UPPER(OutstandingPeriodCode) AS OutstandingPeriodCode
		, DimIsOpenAmountId
		, UPPER(SalesInvoiceCode) AS SalesInvoiceCode
		, UPPER(CustomerCode) AS CustomerCode
		, DefaultExchangeRateTypeCode
		, BudgetExchangeRateTypeCode
		, UPPER(TransactionCurrencyCode) AS TransactionCurrencyCode
		, UPPER(AccountingCurrencyCode) AS AccountingCurrencyCode
		, UPPER(ReportingCurrencyCode) AS ReportingCurrencyCode
		, UPPER(GroupCurrencyCode) AS GroupCurrencyCode
		, InvoiceDate
		, ETL.fn_DateKeyInt(DueDate) AS DimDueDateId
		, LastPaymentDate
		, DocumentDate 
		--, ETL.fn_DateKeyInt(DimCreationDateId) AS DimCreationDateId
		, ReportDate AS DimReportDateId
		, CAST(LEFT(ReportDate, 6) AS int) AS ReportMonthId
		, ReceivablesVoucher
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
		, AppliedExchangeRateAC
		, AppliedExchangeRateRC
		, AppliedExchangeRateGC
		, AppliedExchangeRateAC_Budget
		, AppliedExchangeRateRC_Budget
		, AppliedExchangeRateGC_Budget

FROM DataStore.AccountsReceivable
;
