/****** Object:  Table [DWH].[FactInventory]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DWH`.`FactInventory`(
	`FactInventoryId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`DimProductId` int NOT NULL,
	`DimCompanyId` int NOT NULL,
	`DimProductConfigurationId` int NOT NULL,
	`DimBatchId` int NOT NULL,
	`DimReportDateId` int NOT NULL,
	`DimAccountingCurrencyId` int NOT NULL,
	`DimGroupCurrencyId` int NOT NULL,
	`DimReportingCurrencyId` int NOT NULL,
	`ReportDate` TIMESTAMP NOT NULL,
	`IsEndOfMonth` int NOT NULL,
	`InventoryUnit`  STRING NOT NULL,
	`StockQuantity_PurchaseUnit`  DECIMAL(38,6) ,
	`StockQuantity_InventoryUnit`  DECIMAL(38,6) ,
	`StockQuantity_SalesUnit`  DECIMAL(38,6) ,
	`StockValueAC`  DECIMAL(38,6) ,
	`StockValueRC`  DECIMAL(38,6) ,
	`StockValueGC`  DECIMAL(38,6) ,
	`StockValueAC_Budget`  DECIMAL(38,6) ,
	`StockValueRC_Budget`  DECIMAL(38,6) ,
	`StockValueGC_Budget`  DECIMAL(38,6) ,
	`AppliedExchangeRateRC`  DECIMAL(38,6) ,
	`AppliedExchangeRateAC`  DECIMAL(38,6) ,
	`AppliedExchangeRateGC`  DECIMAL(38,6) ,
	`AppliedExchangeRateRC_Budget`  DECIMAL(38,6) ,
	`AppliedExchangeRateAC_Budget`  DECIMAL(38,6) ,
	`AppliedExchangeRateGC_Budget`  DECIMAL(38,6) ,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
PRIMARY KEY CLUSTERED 
(
	`FactInventoryId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactInventory_DimGroupCurrencyId` FOREIGN KEY(`DimGroupCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactInventory_DimReportingCurrencyId` FOREIGN KEY(`DimReportingCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactInventory_DimAccountingCurrencyId` FOREIGN KEY(`DimAccountingCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactInventory_DimBatch` FOREIGN KEY(`DimBatchId`)
REFERENCES `DWH`.`DimBatch` (`DimBatchId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactInventory_DimCompany` FOREIGN KEY(`DimCompanyId`)
REFERENCES `DWH`.`DimCompany` (`DimCompanyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactInventory_DimProduct` FOREIGN KEY(`DimProductId`)
REFERENCES `DWH`.`DimProduct` (`DimProductId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactInventory_DimProductConfiguration` FOREIGN KEY(`DimProductConfigurationId`)
REFERENCES `DWH`.`DimProductConfiguration` (`DimProductConfigurationId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactInventory_DimReportDateId` FOREIGN KEY(`DimReportDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;
