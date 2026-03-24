/****** Object:  Table [DataStore].[SalesRebate]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore`.`SalesRebate`(
	`CompanyCode`  STRING,
	`SalesRebateCode`  STRING,
	`SalesInvoiceCode`  STRING NOT NULL,
	`SalesInvoiceLineId` bigint NOT NULL,
	`ProductCode`  STRING NOT NULL,
	`RebateCustomerCode`  STRING NOT NULL,
	`RebateCurrencyCode`  STRING NOT NULL,
	`RebateAmountOriginal`  DECIMAL(32,6) NOT NULL,
	`RebateAmountCompleted`  DECIMAL(38,6) ,
	`RebateAmountMarked`  DECIMAL(38,6) ,
	`RebateAmountCancelled`  DECIMAL(38,6) ,
	`RebateAmountVariance`  DECIMAL(38,6) 
)
;
