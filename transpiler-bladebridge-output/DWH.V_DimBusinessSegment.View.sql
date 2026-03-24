/****** Object:  View [DWH].[V_DimBusinessSegment]    Script Date: 03/03/2026 16:26:08 ******/









CREATE OR REPLACE VIEW `DWH`.`V_DimBusinessSegment` AS         


SELECT    BusinessSegmentId
		, UPPER(BusinessSegmentCode) AS BusinessSegmentCode
		, BusinessSegmentName
		, BusinessSegmentCodeName
		, DimensionName         

FROM DataStore.BusinessSegment

/* Create Unknown Member */

UNION ALL 
        
SELECT	-1
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
;
