/****** Object:  Table [DataStore].[LocalAccount]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore`.`LocalAccount`(
	`LocalAccountId` bigint NOT NULL,
	`LocalAccountCode`  STRING NOT NULL,
	`LocalAccountName`  STRING NOT NULL,
	`LocalAccountCodeName`  STRING NOT NULL,
	`DimensionName`  STRING NOT NULL
)
;
