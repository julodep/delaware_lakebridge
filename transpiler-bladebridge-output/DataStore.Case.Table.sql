/****** Object:  Table [DataStore].[Case]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore`.`Case`(
	`CaseCode`  STRING NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`SupplierCode`  STRING NOT NULL,
	`SalesOrderCode`  STRING NOT NULL,
	`PurchaseOrderCode`  STRING NOT NULL,
	`CustomerCode`  STRING NOT NULL,
	`ProductCode`  STRING NOT NULL,
	`CreatedDateTime` TIMESTAMP ,
	`CreatedBy`  STRING NOT NULL,
	`ClosedDateTime` TIMESTAMP ,
	`ClosedBy`  STRING NOT NULL,
	`Description`  STRING NOT NULL,
	`Memo`  STRING NOT NULL,
	`OwnerWorker`  STRING NOT NULL,
	`Priority`  STRING NOT NULL,
	`Process`  STRING NOT NULL,
	`Status`  STRING,
	`PlannedEffectiveDate` TIMESTAMP ,
	`CaseCategoryRecId` bigint NOT NULL,
	`CaseCategoryName`  STRING NOT NULL,
	`CaseCategoryType`  STRING,
	`CaseCategoryDescription`  STRING NOT NULL,
	`CaseCategoryProcess`  STRING NOT NULL
) TEXTIMAGE_ON `PRIMARY`
;
