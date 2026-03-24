/****** Object:  Table [DataStore2].[PurchaseDelivery]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore2`.`PurchaseDelivery`(
	`DeliveriesIdScreening`  STRING NOT NULL,
	`PackingSlipCode`  STRING NOT NULL,
	`PurchaseOrderCode`  STRING NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`ProductConfigurationCode`  STRING NOT NULL,
	`ProductCode`  STRING NOT NULL,
	`OrderSupplierCode`  STRING NOT NULL,
	`SupplierCode`  STRING NOT NULL,
	`DeliveryTermsCode`  STRING NOT NULL,
	`DeliveryModeCode`  STRING NOT NULL,
	`ActualDeliveryDate` TIMESTAMP ,
	`RequestedDeliveryDate` TIMESTAMP ,
	`ConfirmedDeliveryDate` TIMESTAMP ,
	`PurchaseType`  STRING,
	`PurchaseOrderLineNumber` bigint NOT NULL,
	`DeliveryName`  STRING NOT NULL,
	`DeliveryLineNumber`  DECIMAL(32,16) NOT NULL,
	`PurchaseUnit`  STRING,
	`OrderedQuantity_InventoryUnit`  DECIMAL(38,6) NOT NULL,
	`OrderedQuantity_PurchaseUnit`  DECIMAL(38,6) NOT NULL,
	`OrderedQuantity_SalesUnit`  DECIMAL(38,6) NOT NULL,
	`DeliveredQuantity_InventoryUnit`  DECIMAL(38,6) NOT NULL,
	`DeliveredQuantity_PurchaseUnit`  DECIMAL(38,6) NOT NULL,
	`DeliveredQuantity_SalesUnit`  DECIMAL(38,6) NOT NULL
)
;
