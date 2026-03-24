/****** Object:  Table [DWH].[FactInventoryMovements]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`FactInventoryMovements`(
	`FactStockValueId` bigint GENERATED ALWAYS AS IDENTITY NOT NULL,
	`DimProductId` int NOT NULL,
	`DimCompanyId` int NOT NULL,
	`DimBatchId` int NOT NULL,
	`DimProductConfigurationId` int NOT NULL,
	`DimSalesInvoiceId` int NOT NULL,
	`DimStockDateId` INT,
	`StockMonthId` INT,
	`InventoryUnit`  STRING NOT NULL,
	`CurrencyCode`  STRING NOT NULL,
	`Quantity_InventoryUnit`  DECIMAL(38,6) NOT NULL,
	`Quantity_PurchaseUnit`  DECIMAL(38,6) ,
	`Quantity_SalesUnit`  DECIMAL(38,6) NOT NULL,
	`InventoryValueAC`  DECIMAL(38,6) ,
	`InventoryValueRC`  DECIMAL(38,6) ,
	`InventoryValueGC`  DECIMAL(38,6) ,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_FactStockValue` PRIMARY KEY CLUSTERED 
(
	`FactStockValueId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;

WITH CHECK ADD  CONSTRAINT `FK_FactInventoryMovements_DimProductConfiguration` FOREIGN KEY(`DimProductConfigurationId`)
REFERENCES `DWH`.`DimProductConfiguration` (`DimProductConfigurationId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactInventoryMovements_DimSalesInvoice` FOREIGN KEY(`DimSalesInvoiceId`)
REFERENCES `DWH`.`DimSalesInvoice` (`DimSalesInvoiceId`)
;

WITH CHECK ADD  CONSTRAINT `FK_InventoryMovements_Batch` FOREIGN KEY(`DimBatchId`)
REFERENCES `DWH`.`DimBatch` (`DimBatchId`)
;

WITH CHECK ADD  CONSTRAINT `FK_InventoryMovements_Company` FOREIGN KEY(`DimCompanyId`)
REFERENCES `DWH`.`DimCompany` (`DimCompanyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_InventoryMovements_Product` FOREIGN KEY(`DimProductId`)
REFERENCES `DWH`.`DimProduct` (`DimProductId`)
;
