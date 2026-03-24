/****** Object:  Table [DataStore].[Time]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore`.`TIMESTAMP`(
	`DimTimeId` int NOT NULL,
	`HourId` int NOT NULL,
	`HourZoneCode`  STRING NOT NULL,
	`HourZoneName`  STRING NOT NULL,
	`MinuteId` int NOT NULL,
	`TIMESTAMP`  STRING NOT NULL
)
;
