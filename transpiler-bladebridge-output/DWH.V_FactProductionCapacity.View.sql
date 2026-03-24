/****** Object:  View [DWH].[V_FactProductionCapacity]    Script Date: 03/03/2026 16:26:08 ******/






CREATE OR REPLACE VIEW `DWH`.`V_FactProductionCapacity` AS


SELECT	  UPPER(CompanyCode) AS CompanyCode
		, UPPER(CalendarCode) AS CalendarCode
		, UPPER(ResourceCode) AS ResourceCode
		, ETL.fn_DateKeyInt(CapacityDate) AS DimCapacityDateId
		, MaximumCapacity
		, ReservedCapacity
		, AvailableCapacity

FROM DataStore.ProductionCapacity
;
