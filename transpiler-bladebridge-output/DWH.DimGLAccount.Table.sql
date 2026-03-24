/****** Object:  Table [DWH].[DimGLAccount]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DWH`.`DimGLAccount`(
	`DimGLAccountId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`GLAccountId` bigint NOT NULL,
	`GLAccountCode`  STRING NOT NULL,
	`GLAccountName`  STRING NOT NULL,
	`GLAccountType`  STRING NOT NULL,
	`ChartOfAccountsName`  STRING NOT NULL,
	`MainAccountCategory`  STRING NOT NULL,
	`MainAccountCategoryDescription`  STRING NOT NULL,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
	`IsRevenueFlag` `BOOLEAN` ,
	`IsGrossProfitFlag` `BOOLEAN` ,
	`MainAccountCategoryCodeDescription`  STRING NOT NULL,
	`MainAccountCategorySort` INT,
 CONSTRAINT `PK_DimGLAccount` PRIMARY KEY CLUSTERED 
(
	`DimGLAccountId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
