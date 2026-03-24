/****** Object:  Table [DataStore].[GLAccount]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore`.`GLAccount`(
	`CompanyCode`  STRING,
	`GLAccountId` bigint NOT NULL,
	`GLAccountCode`  STRING NOT NULL,
	`GLAccountName`  STRING NOT NULL,
	`GLAccountType`  STRING,
	`ChartOfAccountsName`  STRING NOT NULL,
	`MainAccountCategory`  STRING NOT NULL,
	`MainAccountCategoryDescription`  STRING NOT NULL,
	`MainAccountCategoryCodeDescription`  STRING NOT NULL,
	`MainAccountCategorySort` int NOT NULL,
	`IsRevenueFlag` `BOOLEAN` ,
	`IsGrossProfitFlag` `BOOLEAN` 
)
;
