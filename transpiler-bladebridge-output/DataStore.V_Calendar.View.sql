/****** Object:  View [DataStore].[V_Calendar]    Script Date: 03/03/2026 16:26:09 ******/






CREATE OR REPLACE VIEW `DataStore`.`V_Calendar` AS 


SELECT	  WCS.CalendarId AS CalendarCode
		, WCS.CalendarName AS CalendarName
		, WCS.DataAreaId AS CompanyCode
		, WCS.WorkHours AS StandardWorkDayHours

FROM dbo.SMRBIWorkCalendarStaging WCS
;
