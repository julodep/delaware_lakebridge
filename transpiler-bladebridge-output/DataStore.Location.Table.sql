/****** Object:  Table [DataStore].[Location]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore`.`Location`(
	`LocationId` bigint NOT NULL,
	`LocationCode`  STRING NOT NULL,
	`LocationName`  STRING NOT NULL,
	`LocationCodeName`  STRING NOT NULL,
	`DimensionName`  STRING NOT NULL
)
;
