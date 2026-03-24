/****** Object:  View [DWH].[V_DimCase]    Script Date: 03/03/2026 16:26:09 ******/








CREATE OR REPLACE VIEW `DWH`.`V_DimCase` AS 


SELECT	DISTINCT 
		  UPPER(CaseCode) AS CaseCode
		, UPPER(CompanyCode) AS CompanyCode
		, CAST(CreatedDateTime as TIMESTAMP) AS CreatedDateTime
		, CreatedBy
		, CAST(ClosedDateTime as TIMESTAMP) AS ClosedDateTime
		, ClosedBy
		, Description
		, CAST (Memo as STRING) AS Memo 
		, OwnerWorker
		, Priority
		, Process
		, Status
		, CAST(PlannedEffectiveDate as TIMESTAMP) AS PlannedEffectiveDate
		, CaseCategoryRecId
		, CaseCategoryName
		, CaseCategoryType
		, CaseCategoryDescription
		, CaseCategoryProcess

FROM DataStore.`Case`

UNION ALL 

SELECT	  '_N/A'
		, UPPER(CompanyCode) AS CompanyCode
		, '1900-01-01'
		, '_N/A'
		, '1900-01-01'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '1900-01-01'
		, 0
		, '_N/A'
		, '_N/A'
		, '_N/A'
		, '_N/A'

FROM DataStore.Company
;
