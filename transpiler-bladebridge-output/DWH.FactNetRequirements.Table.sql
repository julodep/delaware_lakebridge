/****** Object:  Table [DWH].[FactNetRequirements]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`FactNetRequirements`(
	`FactNetRequirementsId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`DimCompanyId` int NOT NULL,
	`DimProductId` int NOT NULL,
	`DimProductConfigurationId` int NOT NULL,
	`DimRequirementDateId` int NOT NULL,
	`DimProducedItemId` int NOT NULL,
	`DimCustomerId` int NOT NULL,
	`DimSupplierId` int NOT NULL,
	`ReferenceType`  STRING NOT NULL,
	`RankNr` int NOT NULL,
	`PlanVersion`  STRING NOT NULL,
	`RequirementDateTime` TIMESTAMP NOT NULL,
	`RequirementDateTimeTime` TIMESTAMP NOT NULL,
	`RequirementTime` TIMESTAMP NOT NULL,
	`ReferenceCode`  STRING NOT NULL,
	`ActionDate` TIMESTAMP NOT NULL,
	`ActionDays` int NOT NULL,
	`ActionType`  STRING NOT NULL,
	`ActionMarked`  STRING NOT NULL,
	`FuturesDate` TIMESTAMP NOT NULL,
	`FuturesDays` int NOT NULL,
	`FuturesCalculated`  STRING NOT NULL,
	`FuturesMarked`  STRING NOT NULL,
	`Direction`  STRING NOT NULL,
	`FictionalOOS` `BOOLEAN` NOT NULL,
	`FictionalOOS_Confirmed` `BOOLEAN` NOT NULL,
	`ActualOOS` `BOOLEAN` NOT NULL,
	`ActualOOS_Confirmed` `BOOLEAN` NOT NULL,
	`Quantity_InventoryUnit`  DECIMAL(32,6) NOT NULL,
	`Quantity_PurchaseUnit`  DECIMAL(38,6) ,
	`Quantity_SalesUnit`  DECIMAL(38,6) ,
	`AccumulatedQuantity_InventoryUnit`  DECIMAL(38,6) NOT NULL,
	`AccumulatedQuantity_PurchaseUnit`  DECIMAL(38,6) ,
	`AccumulatedQuantity_SalesUnit`  DECIMAL(38,6) ,
	`QuantityConfirmed_InventoryUnit`  DECIMAL(32,6) NOT NULL,
	`QuantityConfirmed_PurchaseUnit`  DECIMAL(38,6) ,
	`QuantityConfirmed_SalesUnit`  DECIMAL(38,6) NOT NULL,
	`AccumulatedQuantityConfirmed_InventoryUnit`  DECIMAL(38,6) NOT NULL,
	`AccumulatedQuantityConfirmed_PurchaseUnit`  DECIMAL(38,6) ,
	`AccumulatedQuantityConfirmed_SalesUnit`  DECIMAL(38,6) NOT NULL,
	`ProductCostPriceAC`  DECIMAL(24,15) NOT NULL,
	`ProductCostPriceGC`  DECIMAL(24,15) NOT NULL,
	`ValueAC`  DECIMAL(38,6) ,
	`ValueGC`  DECIMAL(38,6) ,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_FactNetRequirements` PRIMARY KEY CLUSTERED 
(
	`FactNetRequirementsId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;

WITH CHECK ADD  CONSTRAINT `FK_FactNetRequirements_DimCompany` FOREIGN KEY(`DimCompanyId`)
REFERENCES `DWH`.`DimCompany` (`DimCompanyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactNetRequirements_DimCustomer` FOREIGN KEY(`DimCustomerId`)
REFERENCES `DWH`.`DimCustomer` (`DimCustomerId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactNetRequirements_DimProducedItem` FOREIGN KEY(`DimProducedItemId`)
REFERENCES `DWH`.`DimProduct` (`DimProductId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactNetRequirements_DimProduct` FOREIGN KEY(`DimProductId`)
REFERENCES `DWH`.`DimProduct` (`DimProductId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactNetRequirements_DimProductConfiguration` FOREIGN KEY(`DimProductConfigurationId`)
REFERENCES `DWH`.`DimProductConfiguration` (`DimProductConfigurationId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactNetRequirements_DimRequirementDate` FOREIGN KEY(`DimRequirementDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactNetRequirements_DimSupplier` FOREIGN KEY(`DimSupplierId`)
REFERENCES `DWH`.`DimSupplier` (`DimSupplierId`)
;
