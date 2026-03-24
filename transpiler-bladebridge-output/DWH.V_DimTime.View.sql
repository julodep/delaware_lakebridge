/****** Object:  View [DWH].[V_DimTime]    Script Date: 03/03/2026 16:26:08 ******/





CREATE OR REPLACE VIEW `DWH`.`V_DimTime` AS


SELECT    DimTimeId
		, HourId
		, HourZoneCode
		, HourZoneName
		, MinuteId
		, `TIMESTAMP`

FROM DataStore.TIMESTAMP
;
