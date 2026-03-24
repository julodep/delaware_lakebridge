/****** Object:  Table [DWH].[DimBudgetModel]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`DimBudgetModel`(
	`DimBudgetModelId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`BudgetModelCode`  STRING NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`BudgetModelName`  STRING NOT NULL,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_DimBudgetModel` PRIMARY KEY CLUSTERED 
(
	`DimBudgetModelId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
