/****** Object:  Table [DataStore].[Date]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore`.TIMESTAMP(
	`DateCalculation`  STRING NOT NULL,
	TIMESTAMP TIMESTAMP NOT NULL,
	`DayNameLong`  STRING NOT NULL,
	`DayNameShort`  STRING NOT NULL,
	`DayOfWeekId` int NOT NULL,
	`DayOfWeekName`  STRING NOT NULL,
	`DayOfYearId` int NOT NULL,
	`DayOfYearName`  STRING NOT NULL,
	`DimDateId` int NOT NULL,
	`MonthId` int NOT NULL,
	`MonthName`  STRING NOT NULL,
	`MonthOfYearId` int NOT NULL,
	`MonthOfYearName`  STRING NOT NULL,
	`QuarterId` int NOT NULL,
	`QuarterName`  STRING NOT NULL,
	`QuarterOfYearId` int NOT NULL,
	`QuarterOfYearName`  STRING NOT NULL,
	`SemesterId` int NOT NULL,
	`SemesterName`  STRING NOT NULL,
	`SemesterOfYearId` int NOT NULL,
	`SemesterOfYearName`  STRING NOT NULL,
	`WeekId` int NOT NULL,
	`WeekName`  STRING NOT NULL,
	`WeekOfYearId` int NOT NULL,
	`WeekOfYearName`  STRING NOT NULL,
	`YearId` int NOT NULL,
	`YearName`  STRING NOT NULL,
	`FYStart` TIMESTAMP 
)
;
