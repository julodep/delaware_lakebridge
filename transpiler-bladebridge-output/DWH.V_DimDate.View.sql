/****** Object:  View [DWH].[V_DimDate]    Script Date: 03/03/2026 16:26:08 ******/

















CREATE OR REPLACE VIEW `DWH`.`V_DimDate` AS



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

	-- FYYEAR
	, FYYearId
	, FYYearName

	-- FYQUARTER
	, FYQuarterOfYearId
	, CONCAT('Q', FYQuarterOfYearId) AS FYQuarterOfYearName	
	, FYQuarterId
	, CONCAT('Q', FYQuarterOfYearId, ' ',YEAR(FYStart)) AS FYQuarterName

	-- FYMONTH
	, FYMonthOfYearId
	, MonthOfYearName AS FYMonthOfYearName
	, FYMonthId
	--, CONCAT(MonthOfYearName, ' ',  FYYearName) AS FYMonthName
	, CONCAT(MonthOfYearName, ' ',  YearName) AS FYMonthName -- 

	-- FYDAY
	, FYDayOfYearId
	--, CONCAT('D',FYDayOfYearId) AS  FYDayOfYearName
	, FYDayId
	, CONCAT(DayOfWeekName, ' ', DayOfYearName , ' ' , FYYearName) AS FYDayNameLong
	, CONCAT(LEFT(DayNameShort,6), FYYearName) AS FYDayNameShort
	, DayOfYearName AS FYDayOfYearName
	, DayOfWeekName AS FYDayOfWeekName
	, DayOfWeekId AS FYDayOfWeekId
	
	-- WEEK
	, FYWeekOfYearid
	, CONCAT('Week ', FYWeekOfYearId) AS FYWeekOfYearName
	,  FYWeekId
	, CONCAT('Week ', FYWeekOfYearId, ' ',YEAR(FYStart)) AS FYWeekName


	-- IS CURRENT/PREVIOUS FYYEAR
	, FYIsCurrentYear = CAST(CASE 
WHEN YEAR(current_timestamp()) = FYYearId
THEN 1
ELSE 0
END AS BOOLEAN)

	, FYIsPreviousYear = CAST(CASE 
WHEN YEAR(current_timestamp()) - 1 = FYYearId
THEN 1
ELSE 0
END AS BOOLEAN)

	, IsCurrentYear = CAST(CASE 
WHEN YEAR(current_timestamp()) = YearId
THEN 1
ELSE 0
END AS BOOLEAN)
	, IsPreviousYear = CAST(CASE 
WHEN YEAR(current_timestamp()) - 1 = YearId
THEN 1
ELSE 0
END AS BOOLEAN)
	, IsToday = CAST(CASE 
WHEN CAST(current_timestamp() AS DATE) = TIMESTAMP
THEN 1
ELSE 0
END AS BOOLEAN)
	, IsYesterday = CAST(CASE 
WHEN CAST(current_timestamp() - INTERVAL 1 DAY AS DATE) = TIMESTAMP
THEN 1
ELSE 0
END AS BOOLEAN)
	, IsCurrentMonth = CAST(CASE 
WHEN YEAR(current_timestamp()) = YearId AND MONTH(current_timestamp()) = MonthOfYearId
THEN 1
ELSE 0
END AS BOOLEAN)
	, IsPreviousMonth = CAST(CASE 
WHEN MONTH(current_timestamp()) = 1 AND YEAR(current_timestamp()) - 1 = YearId AND MonthOfYearId = 1
THEN 1
WHEN YEAR(current_timestamp()) = YearId AND MONTH(current_timestamp()) - 1 = MonthOfYearId
THEN 1
ELSE 0
END AS BOOLEAN)
	, IsCurrentWeek = CAST(CASE 
WHEN YEAR(current_timestamp()) = YearId AND EXTRACT(wk from current_timestamp()) = WeekOfYearId
THEN 1
ELSE 0
END AS BOOLEAN)
	, IsPreviousWeek = CAST(CASE 
WHEN YEAR(current_timestamp()) = YearId AND EXTRACT(wk from current_timestamp()) - 1 = WeekOfYearId
THEN 1
ELSE 0
END AS BOOLEAN)
FROM DatAStore2.DATE

WHERE 1 = 1 AND (YEAR(current_timestamp()) + 10 >= YearId AND DimDateId >= 20080101) -- Adjust if necessary
	OR DimDateId = 19000101


;
