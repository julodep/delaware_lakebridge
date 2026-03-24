/****** Object:  Table [DWH].[FactShipmentInvoice]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`FactShipmentInvoice`(
	`FactShipmentInvoiceId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`DimBusinessOwnerId` int NOT NULL,
	`DimCompanyId` int NOT NULL,
	`DimDepartmentId` int NOT NULL,
	`DimDestinationAgentId` int NOT NULL,
	`DimJobOwnerId` int NOT NULL,
	`DimBusinessSegmentId` int NOT NULL,
	`DimModeOfTransportId` int NOT NULL,
	`DimPurchaseInvoiceId` int NOT NULL,
	`DimSalesInvoiceId` int NOT NULL,
	`DimShipmentContractId` int NOT NULL,
	`DimTransactionCurrencyId` int NOT NULL,
	`DimAccountingCurrencyId` int NOT NULL,
	`DimReportingCurrencyId` int NOT NULL,
	`DimGroupCurrencyId` int NOT NULL,
	`DimCustomerId` int NOT NULL,
	`DimShipmentInvoiceDateId` int NOT NULL,
	`MasterBillOfLading`  STRING,
	`HouseBillOfLading`  STRING,
	`Etd` TIMESTAMP ,
	`Eta` TIMESTAMP ,
	`Description`  STRING,
	`Branch`  STRING,
	`Remark`  STRING,
	`PortOfDestination`  STRING,
	`PortOfOrigin`  STRING,
	`ShipmentInvoiceLineNumber` INT,
	`Voucher`  STRING,
	`ShipmentInvoiceAmountTC`  DECIMAL(38,6) ,
	`ShipmentInvoiceAmountAC`  DECIMAL(38,6) ,
	`ShipmentInvoiceAmountRC`  DECIMAL(38,6) ,
	`ShipmentInvoiceAmountGC`  DECIMAL(38,6) ,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_FactShipmentInvoice` PRIMARY KEY CLUSTERED 
(
	`FactShipmentInvoiceId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;

WITH CHECK ADD  CONSTRAINT `FK_DWH_FactShipmentInvoice_DimAccountingCurrencyId` FOREIGN KEY(`DimAccountingCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_DWH_FactShipmentInvoice_DimBusinessOwnerId` FOREIGN KEY(`DimBusinessOwnerId`)
REFERENCES `DWH`.`DimBusinessOwner` (`DimBusinessOwnerId`)
;

WITH CHECK ADD  CONSTRAINT `FK_DWH_FactShipmentInvoice_DimBusinessSegmentId` FOREIGN KEY(`DimBusinessSegmentId`)
REFERENCES `DWH`.`DimBusinessSegment` (`DimBusinessSegmentId`)
;

WITH CHECK ADD  CONSTRAINT `FK_DWH_FactShipmentInvoice_DimCompanyId` FOREIGN KEY(`DimCompanyId`)
REFERENCES `DWH`.`DimCompany` (`DimCompanyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_DWH_FactShipmentInvoice_DimCustomerId` FOREIGN KEY(`DimCustomerId`)
REFERENCES `DWH`.`DimCustomer` (`DimCustomerId`)
;

WITH CHECK ADD  CONSTRAINT `FK_DWH_FactShipmentInvoice_DimDepartmentId` FOREIGN KEY(`DimDepartmentId`)
REFERENCES `DWH`.`DimDepartment` (`DimDepartmentId`)
;

WITH CHECK ADD  CONSTRAINT `FK_DWH_FactShipmentInvoice_DimDestinationAgentId` FOREIGN KEY(`DimDestinationAgentId`)
REFERENCES `DWH`.`DimDestinationAgent` (`DimDestinationAgentId`)
;

WITH CHECK ADD  CONSTRAINT `FK_DWH_FactShipmentInvoice_DimGroupCurrencyId` FOREIGN KEY(`DimGroupCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_DWH_FactShipmentInvoice_DimJobOwnerId` FOREIGN KEY(`DimJobOwnerId`)
REFERENCES `DWH`.`DimJobOwner` (`DimJobOwnerId`)
;

WITH CHECK ADD  CONSTRAINT `FK_DWH_FactShipmentInvoice_DimModeOfTransportId` FOREIGN KEY(`DimModeOfTransportId`)
REFERENCES `DWH`.`DimDeliveryMode` (`DimDeliveryModeId`)
;

WITH CHECK ADD  CONSTRAINT `FK_DWH_FactShipmentInvoice_DimPurchaseInvoiceId` FOREIGN KEY(`DimPurchaseInvoiceId`)
REFERENCES `DWH`.`DimPurchaseInvoice` (`DimPurchaseInvoiceId`)
;

WITH CHECK ADD  CONSTRAINT `FK_DWH_FactShipmentInvoice_DimReportingCurrencyId` FOREIGN KEY(`DimReportingCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_DWH_FactShipmentInvoice_DimSalesInvoiceId` FOREIGN KEY(`DimSalesInvoiceId`)
REFERENCES `DWH`.`DimSalesInvoice` (`DimSalesInvoiceId`)
;

WITH CHECK ADD  CONSTRAINT `FK_DWH_FactShipmentInvoice_DimShipmentContractId` FOREIGN KEY(`DimShipmentContractId`)
REFERENCES `DWH`.`DimShipmentContract` (`DimShipmentContractId`)
;

WITH CHECK ADD  CONSTRAINT `FK_DWH_FactShipmentInvoice_DimShipmentInvoiceDateId` FOREIGN KEY(`DimShipmentInvoiceDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH CHECK ADD  CONSTRAINT `FK_DWH_FactShipmentInvoice_DimTransactionCurrencyId` FOREIGN KEY(`DimTransactionCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;
