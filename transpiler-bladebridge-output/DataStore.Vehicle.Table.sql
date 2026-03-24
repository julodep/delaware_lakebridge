/****** Object:  Table [DataStore].[Vehicle]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DataStore`.`Vehicle`(
	`VehicleId` bigint NOT NULL,
	`VehicleCode`  STRING NOT NULL,
	`VehicleName`  STRING NOT NULL,
	`VehicleCodeName`  STRING NOT NULL,
	`DimensionName`  STRING NOT NULL
)
;
