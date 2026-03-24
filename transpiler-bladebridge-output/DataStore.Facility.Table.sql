/****** Object:  Table [DataStore].[Facility]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DataStore`.`Facility`(
	`FacilityId` bigint NOT NULL,
	`FacilityCode`  STRING NOT NULL,
	`FacilityName`  STRING NOT NULL,
	`FacilityCodeName`  STRING NOT NULL,
	`DimensionName`  STRING NOT NULL
)
;
