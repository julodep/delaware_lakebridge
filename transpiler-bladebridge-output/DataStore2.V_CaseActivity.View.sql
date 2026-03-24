/****** Object:  View [DataStore2].[V_CaseActivity]    Script Date: 03/03/2026 16:26:09 ******/










CREATE OR REPLACE VIEW `DataStore2`.`V_CaseActivity` AS 


SELECT     CA.CaseCode AS CaseCode
		 , ActivityNumber
		   /* Dimensions */
		 , COALESCE(CA.CompanyCode, '_N/A') AS CompanyCode
		 , COALESCE(C.SupplierCode, '_N/A') AS SupplierCode
		 , COALESCE(C.SalesOrderCode, '_N/A') AS SalesOrderCode
		 , COALESCE(C.PurchaseOrderCode, '_N/A') AS PurchaseOrderCode
		 , UPPER(COALESCE(C.CustomerCode, '_N/A')) AS CustomerCode
		 , COALESCE(C.ProductCode, '_N/A') AS ProductCode
		   /* Dates */
		 , StartDateTime
		 , EndDateTime
		 , ActualEndDateTime
		   /* Case Activity Details */
		 , ActivityTimeType
		 , ActivityTaskTimeType
		 , ActualWork
		 , AllDay
		 , Category
		 , Closed
		 , DoneByWorker
		 , PercentageCompleted
		 , Purpose
		 , ResponsibleWorker
		 , CA.`Status` AS `Status`
		 , TypeCode
		 , UserMemo
FROM Datastore.CaseActivity AS CA

LEFT JOIN DataStore.`Case` AS C
ON C.CaseCode = CA.CaseCode
AND C.CompanyCode = CA.CompanyCode
;
