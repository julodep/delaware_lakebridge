/****** Object:  Table [DWH].[DimWarehouseLocation]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`DimWarehouseLocation`(
	`DimWarehouseLocationId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`WarehouseLocationCode`  STRING NOT NULL,
	`WarehouseCode`  STRING NOT NULL,
	`WarehouseName`  STRING NOT NULL,
	`WarehouseCodeName`  STRING NOT NULL,
	`WarehouseLocationType`  STRING NOT NULL,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_DimWarehouseLocation` PRIMARY KEY CLUSTERED 
(
	`DimWarehouseLocationId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
