/****** Object:  Table [DataStore3].[BadDebtProvision]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DataStore3`.`BadDebtProvision`(
	`CompanyCode`  STRING,
	`DefaultExchangeRateTypeCode`  STRING NOT NULL,
	`BudgetExchangeRateTypeCode`  STRING NOT NULL,
	`TransactionCurrencyCode`  STRING,
	`AccountingCurrencyCode`  STRING NOT NULL,
	`ReportingCurrencyCode`  STRING NOT NULL,
	`GroupCurrencyCode`  STRING NOT NULL,
	`GLAccountCode`  STRING NOT NULL,
	`GLAccountName`  STRING NOT NULL,
	`GLAccountType`  STRING NOT NULL,
	`IntercompanyCode`  STRING NOT NULL,
	`IntercompanyName`  STRING NOT NULL,
	`DepartmentCode`  STRING NOT NULL,
	`DepartmentName`  STRING NOT NULL,
	`EndCustomerCode`  STRING NOT NULL,
	`EndCustomerName`  STRING NOT NULL,
	`LocationCode`  STRING NOT NULL,
	`LocationName`  STRING NOT NULL,
	`ProductFDCode`  STRING NOT NULL,
	`ProductFDName`  STRING NOT NULL,
	`AmountTC`  DECIMAL(32,6) NOT NULL,
	`AmountAC`  DECIMAL(32,6) NOT NULL,
	`AmountGC`  DECIMAL(38,6) NOT NULL,
	`AmountRC`  DECIMAL(32,6) NOT NULL,
	`OutstandingAmountTC`  DECIMAL(38,6) ,
	`OutstandingAmountAC`  DECIMAL(38,6) ,
	`OutstandingAmountGC`  DECIMAL(38,6) ,
	`OutstandingAmountRC`  DECIMAL(38,6) ,
	`OutStandingPeriodCode`  STRING NOT NULL,
	`Voucher`  STRING NOT NULL,
	`SalesInvoiceCode`  STRING NOT NULL,
	`ReportDate` INT,
	`IsDebitCredit`  STRING
)
;
