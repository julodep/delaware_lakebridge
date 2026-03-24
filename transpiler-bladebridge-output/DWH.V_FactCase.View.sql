/****** Object:  View [DWH].[V_FactCase]    Script Date: 03/03/2026 16:26:08 ******/











CREATE OR REPLACE VIEW `DWH`.`V_FactCase` AS


SELECT    UPPER(CaseCode) AS CaseCode
		, UPPER(CompanyCode) AS CompanyCode 
		, UPPER(SupplierCode) AS SupplierCode
		, UPPER(SalesOrderCode) AS SalesOrderCode
		, UPPER(PurchaseOrderCode) AS PurchaseOrderCode
		, UPPER(CustomerCode) AS CustomerCode
		, UPPER(ProductCode) AS ProductCode
		, ETL.fn_DateKeyInt(CreatedDateTime) AS DimCreatedDateTimeId
		, ETL.fn_DateKeyInt(ClosedDateTime) AS DimClosedDateTimeId
		, ETL.fn_DateKeyInt(PlannedEffectiveDate) AS DimPlannedEffectiveDateId
		, CreatedBy
		, ClosedBy
		, Description
		, Memo
		, OwnerWorker
		, Priority
		, Process
		, Status 
		, CaseCategoryRecId
		, CaseCategoryName
		, CaseCategoryType
		, CaseCategoryDescription
		, CaseCategoryProcess

FROM `DataStore`.`Case`
;
