/****** Object:  Table [DataStore].[BudgetCode]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore`.`BudgetCode`(
	`CompanyCode`  STRING,
	`BudgetTransactionCode` bigint NOT NULL,
	`BudgetCodeName`  STRING,
	`BudgetCodeDescription`  STRING NOT NULL
)
;
