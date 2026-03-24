/****** Object:  Table [DataStore].[BusinessUnit]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DataStore`.`BusinessUnit`(
	`BusinessUnitId` bigint NOT NULL,
	`BusinessUnitCode`  STRING NOT NULL,
	`BusinessUnitName`  STRING NOT NULL,
	`BusinessUnitCodeName`  STRING NOT NULL,
	`DimensionName`  STRING NOT NULL
)
;
