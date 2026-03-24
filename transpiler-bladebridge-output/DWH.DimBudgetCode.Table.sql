/****** Object:  Table [DWH].[DimBudgetCode]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`DimBudgetCode`(
	`DimBudgetCodeId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`BudgetCodeName`  STRING NOT NULL,
	`BudgetCodeDescription`  STRING NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`BudgetTransactionCode` bigint NOT NULL,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_DimBudgetCode` PRIMARY KEY CLUSTERED 
(
	`DimBudgetCodeId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
