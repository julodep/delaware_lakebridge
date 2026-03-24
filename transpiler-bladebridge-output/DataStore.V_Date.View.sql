/****** Object:  View [DataStore].[V_Date]    Script Date: 03/03/2026 16:26:08 ******/













CREATE OR REPLACE VIEW `DataStore`.`V_Date` AS


SELECT	DateCalculation
	  , TIMESTAMP
	  , DayNameLong
	  , DayNameShort
	  , DayOfWeekId
	  , DayOfWeekName
	  , DayOfYearId
	  , DayOfYearName
	  , DimDateId
	  , MonthId
	  , MonthName
	  , MonthOfYearId
	  , MonthOfYearName
	  , QuarterId
	  , QuarterName
	  , QuarterOfYearId
	  , QuarterOfYearName
	  , SemesterId
	  , SemesterName
	  , SemesterOfYearId
	  , SemesterOfYearName
	  , WeekId
	  , WeekName
	  , WeekOfYearId
	  , WeekOfYearName
	  , YearId
	  , YearName
	  , CASE WHEN MonthOfYearId > '3' THEN date_format(CONCAT('01/04/' , CAST(YearId AS STRING)), 'dd/MM/yyyy')
	  ELSE date_format(CONCAT('01/04/' , CAST((YearId -1) AS STRING)), 'dd/MM/yyyy') END	  
	  AS FYStart

FROM ETL.Date


;
