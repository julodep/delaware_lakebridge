/****** Object:  Table [DataStore2].[GeneralLedgerBudget]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore2`.`GeneralLedgerBudget`(
	`GeneralBudgetIdScreening`  STRING NOT NULL,
	`TransactionNumber`  STRING NOT NULL,
	`BudgetModelCode`  STRING,
	`BudgetSubModelCode`  STRING,
	`CompanyCode`  STRING,
	`BudgetTransactionCode`  STRING,
	`BudgetType`  STRING NOT NULL,
	`BudgetDate` TIMESTAMP NOT NULL,
	`GLAccountId` bigint NOT NULL,
	`IntercompanyId` bigint NOT NULL,
	`BusinessSegmentId` bigint NOT NULL,
	`DepartmentId` bigint NOT NULL,
	`EndCustomerId` bigint NOT NULL,
	`LocationId` bigint NOT NULL,
	`ShipmentContractId` bigint NOT NULL,
	`LocalAccountId` bigint NOT NULL,
	`ProductFDId` bigint NOT NULL,
	`DefaultExchangeRateTypeCode`  STRING NOT NULL,
	`BudgetExchangeRateTypeCode`  STRING NOT NULL,
	`TransactionCurrencyCode`  STRING,
	`AccountingCurrencyCode`  STRING NOT NULL,
	`ReportingCurrencyCode`  STRING NOT NULL,
	`GroupCurrencyCode`  STRING NOT NULL,
	`BudgetAmountTC`  DECIMAL(34,6) NOT NULL,
	`BudgetAmountAC`  DECIMAL(38,6) NOT NULL,
	`BudgetAmountRC`  DECIMAL(38,6) NOT NULL,
	`BudgetAmountGC`  DECIMAL(38,6) NOT NULL,
	`BudgetAmountAC_Budget`  DECIMAL(38,6) NOT NULL,
	`BudgetAmountRC_Budget`  DECIMAL(38,6) NOT NULL,
	`BudgetAmountGC_Budget`  DECIMAL(38,6) NOT NULL,
	`AppliedExchangeRateTC`  DECIMAL(38,6) ,
	`AppliedExchangeRateAC`  DECIMAL(38,17) NOT NULL,
	`AppliedExchangeRateRC`  DECIMAL(38,17) NOT NULL,
	`AppliedExchangeRateGC`  DECIMAL(38,17) NOT NULL,
	`AppliedExchangeRateAC_Budget`  DECIMAL(38,17) NOT NULL,
	`AppliedExchangeRateRC_Budget`  DECIMAL(38,17) NOT NULL,
	`AppliedExchangeRateGC_Budget`  DECIMAL(38,17) NOT NULL
)
;
