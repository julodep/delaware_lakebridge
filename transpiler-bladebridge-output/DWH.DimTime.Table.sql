/****** Object:  Table [DWH].[DimTime]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`DimTime`(
	`DimTimeId` int NOT NULL,
	`HourId` int NOT NULL,
	`HourZoneCode`  STRING NOT NULL,
	`HourZoneName`  STRING NOT NULL,
	`MinuteId` int NOT NULL,
	`TIMESTAMP`  STRING NOT NULL,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_DimTime` PRIMARY KEY CLUSTERED 
(
	`DimTimeId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
