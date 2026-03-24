/****** Object:  Table [DWH].[FactCase]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`FactCase`(
	`FactCaseId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`DimCaseId` int NOT NULL,
	`DimCompanyId` int NOT NULL,
	`DimSupplierId` int NOT NULL,
	`DimSalesOrderId` int NOT NULL,
	`DimPurchaseOrderId` int NOT NULL,
	`DimCustomerId` int NOT NULL,
	`DimProductId` int NOT NULL,
	`DimCreatedDateTimeId` int NOT NULL,
	`DimClosedDateTimeId` int NOT NULL,
	`DimPlannedEffectiveDateId` int NOT NULL,
	`CreatedBy`  STRING NOT NULL,
	`ClosedBy`  STRING NOT NULL,
	`Description`  STRING NOT NULL,
	`Memo`  STRING NOT NULL,
	`OwnerWorker`  STRING NOT NULL,
	`Priority`  STRING NOT NULL,
	`Process`  STRING NOT NULL,
	`Status`  STRING NOT NULL,
	`CaseCategoryRecId` bigint NOT NULL,
	`CaseCategoryName`  STRING NOT NULL,
	`CaseCategoryType`  STRING NOT NULL,
	`CaseCategoryDescription`  STRING NOT NULL,
	`CaseCategoryProcess`  STRING NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_DWH.FactCase` PRIMARY KEY CLUSTERED 
(
	`FactCaseId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
) TEXTIMAGE_ON `PRIMARY`
;

WITH CHECK ADD  CONSTRAINT `FK_FactCase_DimCase` FOREIGN KEY(`DimCaseId`)
REFERENCES `DWH`.`DimCase` (`DimCaseId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactCase_DimClosedDateTimeId` FOREIGN KEY(`DimClosedDateTimeId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactCase_DimCompany` FOREIGN KEY(`DimCompanyId`)
REFERENCES `DWH`.`DimCompany` (`DimCompanyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactCase_DimCreatedDateTimeId` FOREIGN KEY(`DimCreatedDateTimeId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactCase_DimCustomer` FOREIGN KEY(`DimCustomerId`)
REFERENCES `DWH`.`DimCustomer` (`DimCustomerId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactCase_DimPlannedEffectiveDateId` FOREIGN KEY(`DimPlannedEffectiveDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactCase_DimProduct` FOREIGN KEY(`DimProductId`)
REFERENCES `DWH`.`DimProduct` (`DimProductId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactCase_DimPurchaseOrder` FOREIGN KEY(`DimPurchaseOrderId`)
REFERENCES `DWH`.`DimPurchaseOrder` (`DimPurchaseOrderId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactCase_DimSalesOrder` FOREIGN KEY(`DimSalesOrderId`)
REFERENCES `DWH`.`DimSalesOrder` (`DimSalesOrderId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactCase_DimSupplier` FOREIGN KEY(`DimSupplierId`)
REFERENCES `DWH`.`DimSupplier` (`DimSupplierId`)
;
