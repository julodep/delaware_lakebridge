/****** Object:  Table [DataStore2].[SalesShipment]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DataStore2`.`SalesShipment`(
	`SalesShipmentIdScreening`  STRING NOT NULL,
	`CustPackingSlipCode`  STRING NOT NULL,
	`CustPackingSlipLineNumber`  DECIMAL(32,16) NOT NULL,
	`CustPackingSlipLineNumberCombination`  STRING NOT NULL,
	`SalesOrderCode`  STRING NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`ProductCode`  STRING NOT NULL,
	`CustomerCode`  STRING NOT NULL,
	`OrderCustomerCode`  STRING NOT NULL,
	`InventTransCode`  STRING NOT NULL,
	`InventDimCode`  STRING NOT NULL,
	`SalesUnit`  STRING,
	`RequestedShippingDate` TIMESTAMP ,
	`ConfirmedShippingDate` TIMESTAMP ,
	`ActualDeliveryDate` TIMESTAMP ,
	`OrderedQuantity_InventoryUnit`  DECIMAL(38,6) NOT NULL,
	`OrderedQuantity_PurchaseUnit`  DECIMAL(38,6) NOT NULL,
	`OrderedQuantity_SalesUnit`  DECIMAL(38,6) NOT NULL,
	`DeliveredQuantity_InventoryUnit`  DECIMAL(38,6) NOT NULL,
	`DeliveredQuantity_PurchaseUnit`  DECIMAL(38,6) NOT NULL,
	`DeliveredQuantity_SalesUnit`  DECIMAL(38,6) NOT NULL
)
;
