/****** Object:  Table [DWH].[FactCCC]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`FactCCC`(
	`DimCustomerId` int NOT NULL,
	`DimSupplierId` int NOT NULL,
	`DimProductId` int NOT NULL,
	`DimCompanyId` int NOT NULL,
	`DimReportingDateId` int NOT NULL,
	`DSO` int NOT NULL,
	`DSOCount` int NOT NULL,
	`DPO` int NOT NULL,
	`DPOCount` int NOT NULL,
	`DIO_Volume` int NOT NULL,
	`DIO_Value` int NOT NULL,
	`DIOCount` int NOT NULL,
	`CashConversionCycle` int NOT NULL,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL
)
;

WITH CHECK ADD  CONSTRAINT `FK_FactCCC_DimCompany` FOREIGN KEY(`DimCompanyId`)
REFERENCES `DWH`.`DimCompany` (`DimCompanyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactCCC_DimCustomer` FOREIGN KEY(`DimCustomerId`)
REFERENCES `DWH`.`DimCustomer` (`DimCustomerId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactCCC_DimProduct` FOREIGN KEY(`DimProductId`)
REFERENCES `DWH`.`DimProduct` (`DimProductId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactCCC_DimReportingDate` FOREIGN KEY(`DimReportingDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactCCC_DimSupplier` FOREIGN KEY(`DimSupplierId`)
REFERENCES `DWH`.`DimSupplier` (`DimSupplierId`)
;
