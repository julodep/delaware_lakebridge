/****** Object:  View [DataStore].[V_Case]    Script Date: 03/03/2026 16:26:09 ******/










CREATE OR REPLACE VIEW `DataStore`.`V_Case` AS 

/*CONCATENATE CaseRecId and EntityType*/
;
WITH TEMP1 AS 

(
SELECT CONCAT(CaseRecId,EntityType) AS CaseRecID_EntityType, * 
FROM dbo.SMRBICaseAssociationStaging
),

/* Add according Dimension next to EntityType */

TEMP2 AS
(
SELECT TEMP1.*
	 , CASE WHEN (TEMP1.EntityType = '5')
	 	 	THEN V.VendorAccountNumber 
	 	 	ELSE '_N/A'
	   END AS SupplierCode
	 , COALESCE(V.VendordCreatedDateTime, '1900-01-01 00:00:00.000') AS CreatedDateTimeVendor
	 , CASE WHEN (TEMP1.EntityType = '8')
	 	 	THEN SO.SalesOrderNumber 
	 	 	ELSE '_N/A'
	   END AS SalesOrderId
	 , COALESCE(SO.SalesTableCreatedDateTime, '1900-01-01 00:00:00.000') AS CreatedDateTimeSalesOrder
	 , CASE WHEN (TEMP1.ENTITYTYPE = '9')
	 	 	THEN PO.PurchaseOrderNumber 
	 	 	ELSE '_N/A'
	   END AS PurchaseOrderId				
	 , COALESCE(PO.PURCHTABLECREATEDDATETIME, '1900-01-01 00:00:00.000') AS CreatedDateTimePurchaseOrder
	 , CASE WHEN (TEMP1.EntityType = '4')
	 	 	THEN C.CustomerAccount 
	 	 	ELSE '_N/A'
	   END AS CustomerCode
	 --, ISNULL(C.CreatdDateTime, '1900-01-01') AS CreatedDateTimeCustomer
	 , '1900-01-01 00:00:00.000' AS CreatedDateTimeCustomer
	 , CASE WHEN (TEMP1.EntityType = '11')
	 	 	THEN I.ItemNumber
	 	 	ELSE '_N/A'
	   END AS ProductCode
	 , '1900-01-01 00:00:00.000' AS CreatedDateTimeProduct
	 --, ISNULL(I.CreatedDateTime, '1900-01-01 00:00:00.000') AS CreatedDateTimeProduct

FROM TEMP1

LEFT JOIN dbo.SMRBISalesOrderHeaderStaging AS SO
ON TEMP1.RefRecId = SO.SalesTableRecId
AND TEMP1.EntityType = '8'

LEFT JOIN dbo.SMRBIPurchPurchaseOrderHeaderStaging AS PO
ON TEMP1.RefRecId = PO.PurchTableRecId
AND TEMP1.EntityType = '9'

LEFT JOIN dbo.SMRBICustomerStaging AS C
ON TEMP1.RefRecId = C.CustomerRecId
AND TEMP1.EntityType = '4'

LEFT JOIN dbo.SMRBIVendorStaging AS V
ON TEMP1.RefRecId = V.VendTableRecId
AND TEMP1.EntityType = '5'

LEFT JOIN dbo.SMRBIEcoResReleasedProductStaging AS I
ON TEMP1.RefRecId = I.ProductRecId
AND TEMP1.ENTITYTYPE = '11'
),

/*Create a RowNumber running over each group of CaseRecID_EntityType + Order By ISPRIMARY and Order from oldest to newest EntityTypes*/

TEMP3 AS 
(
SELECT  *,
        ROW_NUMBER() OVER (PARTITION BY TEMP2.CaseRecID_EntityType 
		ORDER BY IsPrimary DESC
			   , CreatedDateTimeVendor
			   , CreatedDateTimeSalesOrder
			   , CreatedDateTimePurchaseOrder
			   , CreatedDateTimeCustomer
			   , CreatedDateTimeProduct
				) AS ROWNUMBER
FROM TEMP2
),

/*Select only the records with RowNuber = 1 to get a 1-to-1 relationship between a case and its dimensions*/

TEMP4 AS
(
SELECT CaseRecId,EntityType,RefRecId 
FROM TEMP3
WHERE ROWNUMBER = 1
),
/*PIVOT*/
TEMP5 AS
(
SELECT CaseRecId
	 , `1` AS EntityType1 
	 , `2` AS EntityType2 
	 , `3` AS EntityType3 
	 , `4` AS EntityType4 
	 , `5` AS EntityType5 
	 , `6` AS EntityType6 
	 , `7` AS EntityType7 
	 , `8` AS EntityType8 
	 , `9` AS EntityType9 
	 , `10` AS EntityType10
	 , `11` AS EntityType11
	 , `12` AS EntityType12
	 , `13` AS EntityType13
	 , `14` AS EntityType14
	 , `15` AS EntityType15
	 , `16` AS EntityType16
	 , `17` AS EntityType17
	 , `18` AS EntityType18
	 , `19` AS EntityType19
	 , `20` AS EntityType20
	 , `21` AS EntityType21
	 , `22` AS EntityType22
	 , `23` AS EntityType23
	 , `24` AS EntityType24
	 , `25` AS EntityType25
	 , `26` AS EntityType26
	 , `27` AS EntityType27
FROM
	(SELECT CaseRecId, EntityType, RefRecId 
	FROM TEMP4) AS Source
PIVOT
(
	MAX(RefRecId)
	FOR EntityType IN (`1`,`2`,`3`,`4`,`5`,`6`,`7`,`8`,`9`,`10`,`11`,`12`,`13`,`14`,`15`,`16`,`17`,`18`,`19`,`20`,`21`,`22`,`23`,`24`,`25`,`26`,`27`)
) AS pvt

)

SELECT COALESCE(NULLIF(C.CASEID,''), '_N/A') AS CaseCode
	   /* Dimensions*/
	 , COALESCE(NULLIF(C.DataAreaId,''), '_N/A') AS  CompanyCode
	 , COALESCE(NULLIF(V.VendorAccountNumber,''), '_N/A') AS SupplierCode
	 , COALESCE(NULLIF(SO.SalesOrderNumber,''), '_N/A') AS SalesOrderCode
	 , COALESCE(NULLIF(PO.PurchaseOrderNumber,''), '_N/A') AS PurchaseOrderCode
	 , COALESCE(NULLIF(CU.CustomerAccount,''), '_N/A') AS CustomerCode
	 , COALESCE(NULLIF(P.ItemNumber,''), '_N/A') AS ProductCode	
	   /* Case Details */
	 , CAST(COALESCE(C.CaseDetailCreatedDateTime, '1900-01-01') AS DATE) AS CreatedDateTime
	 , COALESCE(NULLIF(C.CaseDetailCreatedBy,''), '_N/A') AS CreatedBy
	 , CAST(COALESCE(C.ClosedDateTime, '1900-01-01') AS DATE) AS ClosedDateTime
	 , COALESCE(NULLIF(C.ClosedBy,''), '_N/A') AS ClosedBy
	 , COALESCE(NULLIF(C.Description,''), '_N/A') AS Description
	 , COALESCE(NULLIF(C.Memo,''), '_N/A') AS Memo
	 , COALESCE(NULLIF(W.Name,''), '_N/A') AS OwnerWorker
	 , COALESCE(NULLIF(C.Priority,''), '_N/A') AS Priority
	 , COALESCE(NULLIF(C.Process,''), '_N/A') AS Process
	 , CAST(COALESCE(NULLIF(StringMapCaseStatus.Name,''), '_N/A') AS STRING) AS Status
	 , CAST(COALESCE(C.PlannedEffectiveDate, '1900-01-01') AS DATE) AS PlannedEffectiveDate
	   /* Case Category Details */
 	 , COALESCE(C.CategoryRecId, 0) AS CaseCategoryRecId
	 , COALESCE(NULLIF(CCH.CaseCategory,''), '_N/A') AS CaseCategoryName
	 , CAST(COALESCE(NULLIF(SM.Name,''), '_N/A') AS STRING) AS CaseCategoryType
	 , COALESCE(NULLIF(CCH.Description,''), '_N/A') AS CaseCategoryDescription
	 , COALESCE(NULLIF(CCH.Process,''), '_N/A') AS CaseCategoryProcess

FROM dbo.SMRBICaseDetailBaseStaging AS C

LEFT JOIN TEMP5 AS CA
ON C.CaseDetailBaseRecId = CA.CaseRecId

LEFT JOIN dbo.SMRBICaseCategoryHierarchyDetailStaging AS CCH				
ON C.CategoryRecId = CCH.CaseCategoryHierarchyRecId	
														
LEFT JOIN dbo.SMRBISalesOrderHeaderStaging AS SO
ON CA.EntityType8 = SO.SalesTableRecId

LEFT JOIN dbo.SMRBIHcmWorkerStaging AS W
ON W.HcmWorkerRecId = C.OwnerWorker

LEFT JOIN dbo.SMRBIPurchPurchaseOrderHeaderStaging AS PO
ON CA.EntityType9 = PO.PurchTableRecId

LEFT JOIN dbo.SMRBICustomerStaging AS CU
ON CA.EntityType4 = CU.CustomerRecId

LEFT JOIN dbo.SMRBIVendorStaging AS V
ON CA.EntityType5 = V.VendTableRecId

LEFT JOIN dbo.SMRBIEcoResReleasedProductStaging AS P
ON CA.EntityType11 = P.ProductRecId

--/*To get enum labels*/
LEFT JOIN ETL.StringMap StringMapCaseStatus
ON StringMapCaseStatus.SourceTable = 'CaseStatus'
AND StringMapCaseStatus.Enum = CAST(C.STATUS AS STRING)

LEFT JOIN ETL.StringMap SM
ON SM.SourceTable = 'CaseCategoryType'
;
