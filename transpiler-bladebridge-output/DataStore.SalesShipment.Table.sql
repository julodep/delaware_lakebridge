/****** Object:  Table [DataStore].[SalesShipment]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore`.`SalesShipment`(
	`CustPackingSlipCode`  STRING NOT NULL,
	`CustPackingSlipLineNumber`  DECIMAL(32,16) NOT NULL,
	`CustPackingSlipLineNumberCombination`  STRING NOT NULL,
	`SalesOrderCode`  STRING NOT NULL,
	`SalesInvoiceCode`  STRING NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`ProductCode`  STRING NOT NULL,
	`OrderCustomerCode`  STRING NOT NULL,
	`CustomerCode`  STRING NOT NULL,
	`InventTransCode`  STRING NOT NULL,
	`InventDimCode`  STRING NOT NULL,
	`RequestedShippingDate` TIMESTAMP ,
	`ConfirmedShippingDate` TIMESTAMP ,
	`ActualDeliveryDate` TIMESTAMP ,
	`SalesUnit`  STRING NOT NULL,
	`OrderedQuantity`  DECIMAL(32,6) NOT NULL,
	`DeliveredQuantity`  DECIMAL(32,6) NOT NULL
)
;
