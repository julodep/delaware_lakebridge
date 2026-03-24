/****** Object:  Table [DataStore].[InventoryMovements]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DataStore`.`InventoryMovements`(
	`TransRecId` bigint NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`Currency`  STRING NOT NULL,
	`CostPhysical`  DECIMAL(38,6) ,
	`CostFinancial`  DECIMAL(38,6) ,
	`CostAdjustment`  DECIMAL(38,6) 
)
;
