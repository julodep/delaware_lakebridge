/****** Object:  View [DataStore].[V_Time]    Script Date: 03/03/2026 16:26:08 ******/







CREATE OR REPLACE VIEW `DataStore`.`V_Time` AS


SELECT	DimTimeId
		, HourId
		, HourZoneCode
		, HourZoneName
		, MinuteId
		, `TIMESTAMP`

FROM ETL.TIMESTAMP
;
