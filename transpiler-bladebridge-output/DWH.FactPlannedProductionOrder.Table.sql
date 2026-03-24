/****** Object:  Table [DWH].[FactPlannedProductionOrder]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`FactPlannedProductionOrder`(
	`FactPlannedProductionOrderId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`DimProductId` int NOT NULL,
	`DimCompanyId` int NOT NULL,
	`DimProductConfigurationId` int NOT NULL,
	`DimProductionOrderId` int NOT NULL,
	`DimRequirementDateId` int NOT NULL,
	`RequestedDate` TIMESTAMP NOT NULL,
	`OrderDate` TIMESTAMP NOT NULL,
	`DeliveryDate` TIMESTAMP NOT NULL,
	`PlannedProductionOrderCode`  STRING NOT NULL,
	`Status`  STRING NOT NULL,
	`LeadTime` int NOT NULL,
	`InventoryUnit`  STRING NOT NULL,
	`PurchaseUnit`  STRING NOT NULL,
	`RequirementQuantity_InventoryUnit`  DECIMAL(38,6) ,
	`RequirementQuantity_PurchaseUnit`  DECIMAL(38,6) ,
	`RequirementQuantity_SalesUnit`  DECIMAL(38,6) ,
	`PurchaseQuantity_InventoryUnit`  DECIMAL(38,6) ,
	`PurchaseQuantity_PurchaseUnit`  DECIMAL(38,6) ,
	`PurchaseQuantity_SalesUnit`  DECIMAL(38,6) ,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_FactPlannedProductionOrder` PRIMARY KEY CLUSTERED 
(
	`FactPlannedProductionOrderId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPlannedProductionOrder_DimCompany` FOREIGN KEY(`DimCompanyId`)
REFERENCES `DWH`.`DimCompany` (`DimCompanyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPlannedProductionOrder_DimProduct` FOREIGN KEY(`DimProductId`)
REFERENCES `DWH`.`DimProduct` (`DimProductId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPlannedProductionOrder_DimProductConfiguration` FOREIGN KEY(`DimProductConfigurationId`)
REFERENCES `DWH`.`DimProductConfiguration` (`DimProductConfigurationId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPlannedProductionOrder_DimProductionOrderId` FOREIGN KEY(`DimProductionOrderId`)
REFERENCES `DWH`.`DimProductionOrder` (`DimProductionOrderId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPlannedProductionOrder_DimRequirementDateId` FOREIGN KEY(`DimRequirementDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;
