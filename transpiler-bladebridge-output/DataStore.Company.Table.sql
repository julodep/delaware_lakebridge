/****** Object:  Table [DataStore].[Company]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore`.`Company`(
	`CompanyCode`  STRING,
	`CompanyName`  STRING NOT NULL,
	`CompanyCodeName`  STRING,
	`CompanyType`  STRING NOT NULL
)
;
