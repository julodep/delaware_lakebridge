/****** Object:  Table [DataStore].[PurchaseDelivery]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore`.`PurchaseDelivery`(
	`PackingSlipCode`  STRING NOT NULL,
	`PurchaseOrderCode`  STRING NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`ProductConfigurationCode`  STRING NOT NULL,
	`ProductCode`  STRING NOT NULL,
	`OrderSupplierCode`  STRING NOT NULL,
	`SupplierCode`  STRING NOT NULL,
	`DeliveryModeCode`  STRING NOT NULL,
	`DeliveryTermsCode`  STRING NOT NULL,
	`ActualDeliveryDate` TIMESTAMP ,
	`RequestedDeliveryDate` TIMESTAMP ,
	`ConfirmedDeliveryDate` TIMESTAMP ,
	`PurchaseType`  STRING,
	`PurchaseOrderLineNumber` bigint NOT NULL,
	`DeliveryName`  STRING NOT NULL,
	`DeliveryLineNumber`  DECIMAL(32,16) NOT NULL,
	`PurchaseUnit`  STRING NOT NULL,
	`QuantityOrdered`  DECIMAL(32,6) ,
	`QuantityDelivered`  DECIMAL(32,6) 
)
;
