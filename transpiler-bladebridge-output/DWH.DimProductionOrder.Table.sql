/****** Object:  Table [DWH].[DimProductionOrder]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`DimProductionOrder`(
	`DimProductionOrderId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`ProductionOrderCode`  STRING NOT NULL,
	`ProductionOrderStatus`  STRING NOT NULL,
	`ProductCode`  STRING NOT NULL,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_DimProductionOrder` PRIMARY KEY CLUSTERED 
(
	`DimProductionOrderId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
