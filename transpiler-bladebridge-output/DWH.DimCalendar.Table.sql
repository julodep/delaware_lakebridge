/****** Object:  Table [DWH].[DimCalendar]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`DimCalendar`(
	`DimCalendarId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`CalendarCode`  STRING NOT NULL,
	`CalendarName`  STRING NOT NULL,
	`StandardWorkDayHours`  DECIMAL(32,17) NOT NULL,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_DimCalendar` PRIMARY KEY CLUSTERED 
(
	`DimCalendarId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
