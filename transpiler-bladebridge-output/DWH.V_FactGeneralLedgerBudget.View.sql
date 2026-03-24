/****** Object:  View [DWH].[V_FactGeneralLedgerBudget]    Script Date: 03/03/2026 16:26:08 ******/










CREATE OR REPLACE VIEW `DWH`.`V_FactGeneralLedgerBudget` AS 

/*Adapt the financial dimensions!*/
;
SELECT	  TransactionNumber
		, UPPER(BudgetModelCode) AS BudgetModelCode
		, UPPER(BudgetSubModelCode) AS BudgetSubModelCode
		, UPPER(CompanyCode) AS CompanyCode
		, UPPER(BudgetTransactionCode) AS BudgetTransactionCode
		, UPPER(DefaultExchangeRateTypeCode) AS DefaultExchangeRateTypeCode
		, UPPER(BudgetExchangeRateTypeCode) AS BudgetExchangeRateTypeCode
		, UPPER(TransactionCurrencyCode) AS TransactionCurrencyCode
		, UPPER(AccountingCurrencyCode) AS AccountingCurrencyCode 
		, UPPER(ReportingCurrencyCode) AS ReportingCurrencyCode
		, UPPER(GroupCurrencyCode) AS GroupCurrencyCode
		, GLAccountId
		, IntercompanyId
		, BusinessSegmentId  
		, DepartmentId  
		, EndCustomerId  
		, LocationId  
		, ShipmentContractId  
		, LocalAccountId  
		, ProductFDId 
		, ETL.fn_DateKeyInt(BudgetDate) AS DimBudgetDateId 
		, BudgetType
		, BudgetAmountTC
		, BudgetAmountAC
		, BudgetAmountRC
		, BudgetAmountGC
		, BudgetAmountAC_Budget
		, BudgetAmountRC_Budget
		, BudgetAmountGC_Budget
		, AppliedExchangeRateTC
		, AppliedExchangeRateAC
		, AppliedExchangeRateRC
		, AppliedExchangeRateGC
		, AppliedExchangeRateAC_Budget
		, AppliedExchangeRateRC_Budget
		, AppliedExchangeRateGC_Budget

FROM DataStore2.GeneralLedgerBudget
;
