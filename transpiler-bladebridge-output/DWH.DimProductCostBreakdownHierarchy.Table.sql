/****** Object:  Table [DWH].[DimProductCostBreakdownHierarchy]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`DimProductCostBreakdownHierarchy`(
	`DimProductCostBreakdownHierarchyId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`Level_1`  STRING NOT NULL,
	`Level_2`  STRING NOT NULL,
	`Level_3`  STRING NOT NULL,
	`Dummy` int NOT NULL,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_DimProductCostBreakdownHierarchy` PRIMARY KEY CLUSTERED 
(
	`DimProductCostBreakdownHierarchyId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
