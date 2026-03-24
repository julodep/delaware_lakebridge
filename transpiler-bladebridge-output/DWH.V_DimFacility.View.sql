/****** Object:  View [DWH].[V_DimFacility]    Script Date: 03/03/2026 16:26:09 ******/







CREATE OR REPLACE VIEW `DWH`.`V_DimFacility` AS         


SELECT	  FacilityId
		, UPPER(FacilityCode) AS FacilityCode
		, FacilityName
		, FacilityCodeName
		, DimensionName         

FROM DataStore.Facility

/* Create Unknown Member */

UNION ALL
       
SELECT	-1
	  , '_N/A'
	  , '_N/A'
	  , '_N/A'
	  , '_N/A'
;
