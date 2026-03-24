/****** Object:  Table [DWH].[DimBatch]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`DimBatch`(
	`DimBatchId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`RecId` bigint NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`BatchCode`  STRING NOT NULL,
	`ProductCode`  STRING NOT NULL,
	`Description`  STRING NOT NULL,
	`ExpiryDate`  STRING NOT NULL,
	`ProductionDate`  STRING NOT NULL,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_DimBatch` PRIMARY KEY CLUSTERED 
(
	`DimBatchId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
