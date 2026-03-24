/****** Object:  Table [DataStore2].[ShipmentInvoice]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore2`.`ShipmentInvoice`(
	`BusinessOwnerCode`  STRING NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`DepartmentCode`  STRING NOT NULL,
	`DestinationAgentCode`  STRING NOT NULL,
	`JobOwnerCode`  STRING NOT NULL,
	`LineOfBusinessCode`  STRING NOT NULL,
	`ModeOfTransportCode`  STRING NOT NULL,
	`PurchaseInvoiceCode`  STRING NOT NULL,
	`SalesInvoiceCode`  STRING NOT NULL,
	`ShipmentContractCode`  STRING NOT NULL,
	`TransactionCurrencyCode`  STRING NOT NULL,
	`AccountingCurrencyCode`  STRING NOT NULL,
	`ReportingCurrencyCode`  STRING NOT NULL,
	`GroupCurrencyCode`  STRING NOT NULL,
	`CustomerCode`  STRING NOT NULL,
	`MasterBillOfLading`  STRING NOT NULL,
	`HouseBillOfLading`  STRING NOT NULL,
	`Etd` TIMESTAMP ,
	`Eta` TIMESTAMP ,
	`Description`  STRING NOT NULL,
	`Branch`  STRING NOT NULL,
	`Remark`  STRING NOT NULL,
	`PortOfDestination`  STRING NOT NULL,
	`PortOfOrigin`  STRING NOT NULL,
	`ShipmentInvoiceLineNumber`  DECIMAL(32,16) NOT NULL,
	`ShipmentInvoiceDate` TIMESTAMP ,
	`Amount`  DECIMAL(32,6) NOT NULL,
	`Voucher`  STRING NOT NULL,
	`ShipmentInvoiceAmountTC`  DECIMAL(32,6) NOT NULL,
	`ShipmentInvoiceAmountAC`  DECIMAL(38,6) NOT NULL,
	`ShipmentInvoiceAmountRC`  DECIMAL(38,6) NOT NULL,
	`ShipmentInvoiceAmountGC`  DECIMAL(38,6) NOT NULL
)
;
