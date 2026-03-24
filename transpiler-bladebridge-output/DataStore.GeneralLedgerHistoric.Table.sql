/****** Object:  Table [DataStore].[GeneralLedgerHistoric]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore`.`GeneralLedgerHistoric`(
	`RecId` INT,
	`TransactionCode`  STRING,
	`CompanyCode`  STRING,
	`DefaultExchangeRateTypeCode`  STRING,
	`BudgetExchangeRateTypeCode`  STRING,
	`TransactionCurrencyCode`  STRING,
	`AccountingCurrencyCode`  STRING,
	`ReportingCurrencyCode`  STRING NOT NULL,
	`GroupCurrencyCode`  STRING NOT NULL,
	`GLAccountCode`  STRING,
	`InterCompanyCode`  STRING,
	`BusinessSegmentCode`  STRING,
	`DepartmentCode`  STRING,
	`EndCustomerCode`  STRING,
	`LocationCode`  STRING,
	`ShipmentContractCode`  STRING,
	`LocalAccountCode`  STRING,
	`ProductFDCode`  STRING,
	`DocumentDate` TIMESTAMP ,
	`DimPostingDateId` INT,
	`Voucher`  STRING NOT NULL,
	`AmountTC`  DECIMAL(38,6) NOT NULL,
	`AmountAC`  DECIMAL(38,6) ,
	`AmountRC`  DECIMAL(38,6) ,
	`AmountGC`  DECIMAL(38,6) NOT NULL,
	`AppliedExchangeRateTC`  DECIMAL(38,21) NOT NULL,
	`AppliedExchangeRateAC`  DECIMAL(38,17) ,
	`AppliedExchangeRateRC`  DECIMAL(38,17) ,
	`AppliedExchangeRateGC`  DECIMAL(38,21) NOT NULL
)
;
