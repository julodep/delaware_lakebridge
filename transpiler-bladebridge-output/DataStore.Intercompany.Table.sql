/****** Object:  Table [DataStore].[Intercompany]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore`.`Intercompany`(
	`IntercompanyId` bigint NOT NULL,
	`IntercompanyCode`  STRING NOT NULL,
	`IntercompanyName`  STRING NOT NULL,
	`IntercompanyCodeName`  STRING NOT NULL,
	`DimensionName`  STRING NOT NULL
)
;
