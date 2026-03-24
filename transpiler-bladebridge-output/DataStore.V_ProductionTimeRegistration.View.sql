/****** Object:  View [DataStore].[V_ProductionTimeRegistration]    Script Date: 03/03/2026 16:26:09 ******/








CREATE OR REPLACE VIEW `DataStore`.`V_ProductionTimeRegistration` AS


SELECT COALESCE(NULLIF(PRT.TransRefId,''), '_N/A') AS ProductionOrderCode
	 , COALESCE(NULLIF(UPPER(PRT.Company),''), '_N/A') AS CompanyCode
	 , COALESCE(NULLIF(UPPER(PT.InventDimId),''), '_N/A') AS ProductConfigurationCode
	 , COALESCE(NULLIF(PT.RouteId,''), '_N/A') AS RouteCode
	 , COALESCE(NULLIF(RHS.RouteName,''), '_N/A') AS RoutingName
	 , COALESCE(NULLIF(PRT.WrkCtrId,''), '_N/A') AS ResourceCode
	 , COALESCE(NULLIF(PRT.OPRId,''), '_N/A') AS OperationCode
	 , COALESCE(PRT.OPRNum, -1) AS OperationNumber
	 , N'_N/A' AS Shift
	 , COALESCE(NULLIF(PRT.CategoryId,''), '_N/A') AS OperatorType
	 , COALESCE(PRT.Worker, -1) AS OperatorName
	 , PRT.ProdRouteTransRecId AS RecId

	   /*Dates*/
	 , CAST(COALESCE(PRT.DateWip, '1900-01-01') AS DATE) AS PostedJournalDate

	   /*Key figures*/
	 , PRT.Hours AS Hours
	 , PRT.HourPrice AS HourPrice

FROM dbo.SMRBIProdRouteTransStaging PRT

LEFT JOIN dbo.SMRBIProdTableStaging PT
ON PT.Company = PRT.Company
AND PT.ProdId = PRT.TransRefId

LEFT JOIN dbo.SMRBIRouteHeaderStaging RHS
ON RHS.Company = PRT.Company
AND RHS.RouteId = PT.RouteId
;
