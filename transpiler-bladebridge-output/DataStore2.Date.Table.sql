/****** Object:  Table [DataStore2].[Date]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore2`.TIMESTAMP(
	`DimDateId` int NOT NULL,
	TIMESTAMP TIMESTAMP NOT NULL,
	`YearId` int NOT NULL,
	`YearName`  STRING NOT NULL,
	`SemesterId` int NOT NULL,
	`SemesterName`  STRING NOT NULL,
	`SemesterOfYearId` int NOT NULL,
	`SemesterOfYearName`  STRING NOT NULL,
	`QuarterOfYearId` int NOT NULL,
	`QuarterId` int NOT NULL,
	`QuarterName`  STRING NOT NULL,
	`QuarterOfYearName`  STRING NOT NULL,
	`MonthOfYearId` int NOT NULL,
	`MonthId` int NOT NULL,
	`MonthName`  STRING NOT NULL,
	`MonthOfYearName`  STRING NOT NULL,
	`DayOfYearId` int NOT NULL,
	`DayNameLong`  STRING NOT NULL,
	`DayNameShort`  STRING NOT NULL,
	`DayOfYearName`  STRING NOT NULL,
	`DayOfWeekName`  STRING NOT NULL,
	`DayOfWeekId` int NOT NULL,
	`WeekOfYearId` int NOT NULL,
	`WeekOfYearName`  STRING NOT NULL,
	`WeekId` int NOT NULL,
	`WeekName`  STRING NOT NULL,
	`FYStart` TIMESTAMP ,
	`FYYearId` INT,
	`FYYearName` INT,
	`FYDayOfYearId` int NOT NULL,
	`FYDayId`  STRING NOT NULL,
	`FYQuarterOfYearId` INT,
	`FYQuarterId`  STRING NOT NULL,
	`FYMonthOfYearId` INT,
	`FYMonthId`  STRING NOT NULL,
	`FYWeekOfYearId`  DECIMAL(22,0) ,
	`FYWeekId`  STRING NOT NULL
)
;
