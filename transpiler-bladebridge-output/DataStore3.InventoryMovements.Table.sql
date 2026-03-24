/****** Object:  Table [DataStore3].[InventoryMovements]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DataStore3`.`InventoryMovements`(
	`RecId` bigint NOT NULL,
	`TransType` INT,
	`InventTransCode`  STRING,
	`InventDimCode`  STRING NOT NULL,
	`CompanyCode`  STRING,
	`ProductCode`  STRING,
	`WarehouseLocationCode`  STRING NOT NULL,
	`InventLocationCode`  STRING NOT NULL,
	`InventBatchCode`  STRING NOT NULL,
	`SalesInvoiceCode`  STRING NOT NULL,
	`DatePhysical` TIMESTAMP NOT NULL,
	`DateFinancial` TIMESTAMP NOT NULL,
	`DateClosed` TIMESTAMP NOT NULL,
	`InventoryUnit`  STRING NOT NULL,
	`Quantity_InventoryUnit`  DECIMAL(38,6) NOT NULL,
	`Quantity_SalesUnit`  DECIMAL(38,6) NOT NULL,
	`Quantity_PurchaseUnit`  DECIMAL(38,6) NOT NULL,
	`Currency`  STRING NOT NULL,
	`CostPhysicalTC`  DECIMAL(38,6) NOT NULL,
	`CostPhysicalAC`  DECIMAL(38,6) NOT NULL,
	`CostPhysicalRC`  DECIMAL(38,6) NOT NULL,
	`CostPhysicalGC`  DECIMAL(38,6) NOT NULL,
	`CostFinancialTC`  DECIMAL(38,6) NOT NULL,
	`CostFinancialAC`  DECIMAL(38,6) NOT NULL,
	`CostFinancialRC`  DECIMAL(38,6) NOT NULL,
	`CostFinancialGC`  DECIMAL(38,6) NOT NULL,
	`PriceMatch` int NOT NULL,
	`AppliedExchangeRateTC`  DECIMAL(38,6) ,
	`AppliedExchangeRateRC`  DECIMAL(38,6) ,
	`AppliedExchangeRateAC`  DECIMAL(38,6) ,
	`AppliedExchangeRateGC`  DECIMAL(38,21) NOT NULL
)
;
