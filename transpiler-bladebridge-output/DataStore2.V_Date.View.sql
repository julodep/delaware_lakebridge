/****** Object:  View [DataStore2].[V_Date]    Script Date: 03/03/2026 16:26:09 ******/







CREATE OR REPLACE VIEW `DataStore2`.`V_Date` AS 


SELECT DimDateId
	, TIMESTAMP
	, YearId
	, YearName
	, SemesterId
	, SemesterName
	, SemesterOfYearId
	, SemesterOfYearName
	, QuarterOfYearId
	, QuarterId
	, QuarterName
	, QuarterOfYearName
	, MonthOfYearId
	, MonthId
	, `MonthName`
	, MonthOfYearName
	, DayOfYearId
	, DayNameLong
	, DayNameShort
	, DayOfYearName
	, DayOfWeekName
	, DayOfWeekId
	, WeekOfYearId
	, WeekOfYearName
	, WeekId
	, WeekName
	, FYStart

	--YEAR
	, YEAR(FYStart) AS FYYearId
	, YEAR(FYStart) AS FYYearName
	

	-- DAY
-- DATEDIFF(DAY, FYStart, DATETIME) + 1 AS  FYDayOfYearId
	, DayOfYearId AS FYDayOfYearId
	, CASE 
		WHEN DATEDIFF(FYStart, TIMESTAMP) + 1 < 10
			THEN CONCAT (YEAR(FYStart), '0', DATEDIFF(FYStart, TIMESTAMP) + 1)
		ELSE CONCAT (YEAR(FYStart), DATEDIFF(FYStart, TIMESTAMP) + 1)
		END AS FYDayId

	-- QUARTER

	, DATEDIFF(QUARTER, TIMESTAMP, FYStart) + 1 AS FYQuarterOfYearId
	
	, CONCAT (YEAR(FYStart), CONCAT ('0', DATEDIFF(QUARTER, TIMESTAMP, FYStart) +1)) AS FYQuarterId
	
	-- MONTH
	, CAST(MONTHS_BETWEEN(FYStart, TIMESTAMP) AS INT) + 1 AS FYMonthOfYearId
	
	, CASE 
		WHEN (CAST(MONTHS_BETWEEN(FYStart, TIMESTAMP) AS INT) + 1) < 10
			THEN CONCAT (YEAR(FYStart), '0', (CAST(MONTHS_BETWEEN(FYStart, TIMESTAMP) AS INT) + 1)
					)
		ELSE CONCAT (YEAR(FYStart), CAST(MONTHS_BETWEEN(FYStart, TIMESTAMP) AS INT) + 1)
		END AS FYMonthId

	-- WEEK
	, CEILING(CAST(DATEDIFF(FYStart, TIMESTAMP) + 1 + EXTRACT(DW from FYStart) - 1 AS DECIMAL(20,8)) / 7) AS FYWeekOfYearId

	, CASE 
		WHEN CEILING(CAST(DATEDIFF(FYStart, TIMESTAMP) + 1 + EXTRACT(DW from FYStart) - 1 AS DECIMAL(20, 8)) / 7) < 10
			THEN CONCAT (YEAR(FYStart), '0', CEILING(CAST(DATEDIFF(FYStart, TIMESTAMP) + 1 + EXTRACT(DW from FYStart) - 1 AS DECIMAL(20, 8)) / 7)
					)
		ELSE CONCAT (YEAR(FYStart), CEILING(CAST(DATEDIFF(FYStart, TIMESTAMP) + 1 + EXTRACT(DW from FYStart) - 1 AS DECIMAL(20, 8)) / 7))
		END AS FYWeekId

		FROM DatAStore.DATE
;
