/****** Object:  View [DWH].[V_DimCalendar]    Script Date: 03/03/2026 16:26:09 ******/










CREATE OR REPLACE VIEW `DWH`.`V_DimCalendar` AS


SELECT	  UPPER(CalendarCode) AS CalendarCode
		, UPPER(CompanyCode) AS CompanyCode
		, CalendarName
		, StandardWorkDayHours

FROM DataStore.Calendar

/*Create Unknown member */

UNION ALL

SELECT	'_N/A'
		,   UPPER(CompanyCode)
	  , '_N/A'
	
	  , 0
FROM DataStore.Company
;
