/****** Object:  Table [DataStore2].[CaseActivity]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore2`.`CaseActivity`(
	`CaseCode`  STRING NOT NULL,
	`ActivityNumber`  STRING NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`SupplierCode`  STRING NOT NULL,
	`SalesOrderCode`  STRING NOT NULL,
	`PurchaseOrderCode`  STRING NOT NULL,
	`CustomerCode`  STRING,
	`ProductCode`  STRING NOT NULL,
	`StartDateTime` TIMESTAMP ,
	`EndDateTime` TIMESTAMP ,
	`ActualEndDateTime` TIMESTAMP ,
	`ActivityTimeType`  STRING,
	`ActivityTaskTimeType`  STRING,
	`ActualWork`  DECIMAL(32,6) NOT NULL,
	`AllDay`  STRING,
	`Category`  STRING,
	`Closed`  STRING,
	`DoneByWorker`  STRING NOT NULL,
	`PercentageCompleted`  DECIMAL(32,6) NOT NULL,
	`Purpose`  STRING NOT NULL,
	`ResponsibleWorker`  STRING NOT NULL,
	`Status`  STRING,
	`TypeCode`  STRING NOT NULL,
	`UserMemo`  STRING NOT NULL
) TEXTIMAGE_ON `PRIMARY`
;
