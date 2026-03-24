/****** Object:  Table [DWH].[DimCostCenter]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`DimCostCenter`(
	`DimCostCenterId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`CostCenterId` bigint NOT NULL,
	`CostCenterCode`  STRING NOT NULL,
	`CostCenterName`  STRING NOT NULL,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_DimCostCenter` PRIMARY KEY CLUSTERED 
(
	`DimCostCenterId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
