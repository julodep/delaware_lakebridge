/****** Object:  Table [ETL].[Time]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `ETL`.`TIMESTAMP`(
	`DimTimeId` int NOT NULL,
	`TIMESTAMP`  STRING NOT NULL,
	`HourId` int NOT NULL,
	`MinuteId` int NOT NULL,
	`HourZoneCode`  STRING NOT NULL,
	`HourZoneName`  STRING NOT NULL
)
;
