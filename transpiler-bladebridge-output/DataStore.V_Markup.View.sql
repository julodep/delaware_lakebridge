/****** Object:  View [DataStore].[V_Markup]    Script Date: 03/03/2026 16:26:09 ******/









CREATE OR REPLACE VIEW `DataStore`.`V_Markup` AS


SELECT	CompanyId AS CompanyCode
	  , TransRecId AS TransRecId
	  , MarkupCategory AS MarkupCategory
	  , TransTableId AS TransTableCode
	  , COALESCE(PivotTable.`TRANSPORT`, 0) AS SurchargeTransport /* ADD/ALTER if required */
	  , COALESCE(PivotTable.`aankoop`, 0) AS SurchargePurchase /* ADD/ALTER if required */
	  , COALESCE(PivotTable.`DlvCost1`, 0) AS SurchargeDelivery /* ADD/ALTER if required */
	  , COALESCE(`TRANSPORT`, 0) + COALESCE(`aankoop`, 0) + COALESCE(`DlvCost1`, 0) AS SurchargeTotal
FROM(
	SELECT	UPPER(DataAreaId) AS CompanyId
		  , MarkupCode
		  , TransRecId
		  , `Value`
		  , MarkupCategory
		  , TransTableId
	FROM dbo.SMRBIMarkupTransStaging
	WHERE 1=1
	AND TransTableId IN (SELECT TableId FROM ETL.SqlDictionary)
	) SourceTable

	PIVOT(MIN(Value) FOR MarkupCode IN (`TRANSPORT`,`aankoop`,`DlvCost1`) /* ADD/ALTER if required */ ) AS PivotTable
;
