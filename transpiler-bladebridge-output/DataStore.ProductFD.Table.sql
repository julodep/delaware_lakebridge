/****** Object:  Table [DataStore].[ProductFD]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DataStore`.`ProductFD`(
	`ProductFDId` bigint NOT NULL,
	`ProductFDCode`  STRING NOT NULL,
	`ProductFDName`  STRING NOT NULL,
	`ProductFDCodeName`  STRING NOT NULL,
	`DimensionName`  STRING NOT NULL
)
;
