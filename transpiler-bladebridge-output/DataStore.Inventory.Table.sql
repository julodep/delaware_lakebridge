/****** Object:  Table [DataStore].[Inventory]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore`.`Inventory`(
	`ProductCode`  STRING NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`ProductConfigurationCode`  STRING NOT NULL,
	`BatchCode`  STRING NOT NULL,
	`ReportDate` TIMESTAMP ,
	`DefaultExchangeRateTypeCode`  STRING NOT NULL,
	`BudgetExchangeRateTypeCode`  STRING NOT NULL,
	`AccountingCurrencyCode`  STRING NOT NULL,
	`ReportingCurrencyCode`  STRING NOT NULL,
	`GroupCurrencyCode`  STRING NOT NULL,
	`InventoryUnit`  STRING NOT NULL,
	`StockQuantity`  DECIMAL(32,6) NOT NULL,
	`StockValueAC`  DECIMAL(38,6) NOT NULL,
	`StockValueRC`  DECIMAL(38,6) NOT NULL,
	`StockValueGC`  DECIMAL(38,6) NOT NULL,
	`StockValueAC_Budget`  DECIMAL(38,6) NOT NULL,
	`StockValueRC_Budget`  DECIMAL(38,6) NOT NULL,
	`StockValueGC_Budget`  DECIMAL(38,6) NOT NULL,
	`AppliedExchangeRateRC`  DECIMAL(38,21) NOT NULL,
	`AppliedExchangeRateAC`  DECIMAL(38,21) NOT NULL,
	`AppliedExchangeRateGC`  DECIMAL(38,21) NOT NULL,
	`AppliedExchangeRateRC_Budget`  DECIMAL(38,21) NOT NULL,
	`AppliedExchangeRateAC_Budget`  DECIMAL(38,21) NOT NULL,
	`AppliedExchangeRateGC_Budget`  DECIMAL(38,21) NOT NULL
)
;
