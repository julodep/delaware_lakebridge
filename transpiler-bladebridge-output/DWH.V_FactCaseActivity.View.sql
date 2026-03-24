/****** Object:  View [DWH].[V_FactCaseActivity]    Script Date: 03/03/2026 16:26:08 ******/






CREATE OR REPLACE VIEW `DWH`.`V_FactCaseActivity` AS


SELECT    ActivityNumber
		, UPPER(CaseCode) AS CaseCode
		, UPPER(CompanyCode) AS CompanyCode
		, UPPER(SupplierCode) AS SupplierCode
		, UPPER(SalesOrderCode) AS SalesOrderCode
		, UPPER(PurchaseOrderCode) AS PurchaseOrderCode
		, UPPER(CustomerCode) AS CustomerCode
		, UPPER(ProductCode) AS ProductCode
		, ETL.fn_DateKeyInt(StartDateTime) AS DimStartDateTimeId
		, ETL.fn_DateKeyInt(EndDateTime) AS DimEndDateTimeId
		, ETL.fn_DateKeyInt(ActualEndDateTime) AS DimActualEndDateTimeId
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
		, Status
		, TypeCode
		, UserMemo

FROM DataStore2.CaseActivity
;
