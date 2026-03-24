/****** Object:  Table [DWH].[FactCaseActivity]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`FactCaseActivity`(
	`FactCaseActivityId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`DimCaseId` int NOT NULL,
	`DimCompanyId` int NOT NULL,
	`DimSupplierId` int NOT NULL,
	`DimSalesOrderId` int NOT NULL,
	`DimPurchaseOrderId` int NOT NULL,
	`DimProductId` int NOT NULL,
	`DimCustomerId` int NOT NULL,
	`DimStartDateTimeId` int NOT NULL,
	`DimEndDateTimeId` int NOT NULL,
	`DimActualEndDateTimeId` int NOT NULL,
	`ActivityNumber`  STRING NOT NULL,
	`ActivityTimeType`  STRING NOT NULL,
	`ActivityTaskTimeType`  STRING NOT NULL,
	`ActualWork`  DECIMAL(32,6) NOT NULL,
	`AllDay`  STRING NOT NULL,
	`Category`  STRING NOT NULL,
	`Closed`  STRING NOT NULL,
	`DoneByWorker`  STRING NOT NULL,
	`PercentageCompleted`  DECIMAL(32,6) NOT NULL,
	`Purpose`  STRING NOT NULL,
	`ResponsibleWorker`  STRING NOT NULL,
	`Status`  STRING NOT NULL,
	`TypeCode`  STRING NOT NULL,
	`UserMemo`  STRING NOT NULL,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_FactCaseActivity` PRIMARY KEY CLUSTERED 
(
	`FactCaseActivityId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
) TEXTIMAGE_ON `PRIMARY`
;

WITH CHECK ADD  CONSTRAINT `FK_FactCaseActivity_DimCase` FOREIGN KEY(`DimCaseId`)
REFERENCES `DWH`.`DimCase` (`DimCaseId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactCaseActivity_DimCompany` FOREIGN KEY(`DimCompanyId`)
REFERENCES `DWH`.`DimCompany` (`DimCompanyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactCaseActivity_DimCustomer` FOREIGN KEY(`DimCustomerId`)
REFERENCES `DWH`.`DimCustomer` (`DimCustomerId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactCaseActivity_DimProduct` FOREIGN KEY(`DimProductId`)
REFERENCES `DWH`.`DimProduct` (`DimProductId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactCaseActivity_DimPurchaseOrder` FOREIGN KEY(`DimPurchaseOrderId`)
REFERENCES `DWH`.`DimPurchaseOrder` (`DimPurchaseOrderId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactCaseActivity_DimSalesOrder` FOREIGN KEY(`DimSalesOrderId`)
REFERENCES `DWH`.`DimSalesOrder` (`DimSalesOrderId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactCaseActivity_DimSupplier` FOREIGN KEY(`DimSupplierId`)
REFERENCES `DWH`.`DimSupplier` (`DimSupplierId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurch_DimActualEndDateTimeId` FOREIGN KEY(`DimActualEndDateTimeId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurch_DimEndDateTimeId` FOREIGN KEY(`DimEndDateTimeId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurch_DimStartDateTimeId` FOREIGN KEY(`DimStartDateTimeId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;
