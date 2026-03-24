/****** Object:  Table [DWH].[DimProductConfiguration]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`DimProductConfiguration`(
	`DimProductConfigurationId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`CompanyCode`  STRING NOT NULL,
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
	`WarehouseLocationCode`  STRING NOT NULL,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_DimProductConfiguration` PRIMARY KEY CLUSTERED 
(
	`DimProductConfigurationId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
