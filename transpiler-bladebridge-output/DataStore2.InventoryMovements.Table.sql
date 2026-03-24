/****** Object:  Table [DataStore2].[InventoryMovements]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore2`.`InventoryMovements`(
	`RecId` bigint NOT NULL,
	`TransType` INT,
	`InventTransCode`  STRING,
	`InventDimCode`  STRING NOT NULL,
	`ReferenceCode`  STRING,
	`SalesInvoiceCode`  STRING NOT NULL,
	`CompanyCode`  STRING,
	`ProductCode`  STRING,
	`WarehouseLocationCode`  STRING NOT NULL,
	`InventLocationCode`  STRING NOT NULL,
	`InventBatchCode`  STRING NOT NULL,
	`DatePhysical` TIMESTAMP NOT NULL,
	`DateFinancial` TIMESTAMP NOT NULL,
	`DateClosed` TIMESTAMP NOT NULL,
	`CurrencyCode`  STRING,
	`InventoryUnit`  STRING NOT NULL,
	`QTY`  DECIMAL(32,6) NOT NULL,
	`CostPhysicalTC`  DECIMAL(38,6) ,
	`CostFinancialTC`  DECIMAL(38,6) ,
	`PriceMatch` int NOT NULL
)
;
