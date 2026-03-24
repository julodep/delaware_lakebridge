/****** Object:  Table [DataStore].[ProductConfiguration]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore`.`ProductConfiguration`(
	`CompanyCode`  STRING,
	`InventDimCode`  STRING NOT NULL,
	`ProductConfigurationCode`  STRING NOT NULL,
	`InventBatchCode`  STRING NOT NULL,
	`InventColorCode`  STRING NOT NULL,
	`InventSizeCode`  STRING NOT NULL,
	`InventStyleCode`  STRING NOT NULL,
	`InventStatusCode`  STRING NOT NULL,
	`SiteCode`  STRING NOT NULL,
	`SiteName`  STRING NOT NULL,
	`WarehouseCode`  STRING NOT NULL,
	`WarehouseName`  STRING NOT NULL,
	`WarehouseLocationCode`  STRING NOT NULL
)
;
