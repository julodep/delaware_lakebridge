/****** Object:  Table [DataStore].[ProductCostBreakdownHierarchy]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore`.`ProductCostBreakdownHierarchy`(
	`CompanyCode`  STRING,
	`Level_1`  STRING NOT NULL,
	`Level_2`  STRING NOT NULL,
	`Level_3`  STRING NOT NULL
)
;
