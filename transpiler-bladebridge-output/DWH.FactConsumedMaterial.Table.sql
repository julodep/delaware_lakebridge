/****** Object:  Table [DWH].[FactConsumedMaterial]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`FactConsumedMaterial`(
	`FactConsumedMaterialId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`DimCompanyId` int NOT NULL,
	`DimProductId` int NOT NULL,
	`DimMonthId` int NOT NULL,
	`DimProductConfigurationId` int NOT NULL,
	`RealCostAmountAC`  DECIMAL(38,6) ,
	`RealCostAmountGC`  DECIMAL(38,6) ,
	`RealConsumptionQuantity_InventoryUnit`  DECIMAL(38,6) ,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_FactConsumedMaterial` PRIMARY KEY CLUSTERED 
(
	`FactConsumedMaterialId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;

WITH CHECK ADD  CONSTRAINT `FK_FactConsumedMaterial_DimCompany` FOREIGN KEY(`DimCompanyId`)
REFERENCES `DWH`.`DimCompany` (`DimCompanyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactConsumedMaterial_DimProduct` FOREIGN KEY(`DimProductId`)
REFERENCES `DWH`.`DimProduct` (`DimProductId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactConsumedMaterial_DimProductConfiguration` FOREIGN KEY(`DimProductConfigurationId`)
REFERENCES `DWH`.`DimProductConfiguration` (`DimProductConfigurationId`)
;
