/****** Object:  Table [DataStore].[WarehouseLocation]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DataStore`.`WarehouseLocation`(
	`RecId` bigint NOT NULL,
	`CompanyCode`  STRING,
	`WarehouseLocationCode`  STRING NOT NULL,
	`WarehouseCode`  STRING,
	`WarehouseName`  STRING NOT NULL,
	`WarehouseCodeName`  STRING,
	`WareHouseLocationType`  STRING NOT NULL
)
;
