/****** Object:  Table [DataStore2].[PlannedPurchaseOrder]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DataStore2`.`PlannedPurchaseOrder`(
	`PlannedPurchaseOrderCode`  STRING NOT NULL,
	`ProductCode`  STRING NOT NULL,
	`SupplierCode`  STRING NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`ProductConfigurationCode`  STRING NOT NULL,
	`PurchaseOrderCode`  STRING NOT NULL,
	`RequirementDate` TIMESTAMP NOT NULL,
	`RequestedDate` TIMESTAMP NOT NULL,
	`OrderDate` TIMESTAMP NOT NULL,
	`DeliveryDate` TIMESTAMP ,
	`Status`  STRING,
	`LeadTime` int NOT NULL,
	`InventoryUnit`  STRING NOT NULL,
	`PurchaseUnit`  STRING NOT NULL,
	`RequirementQuantity_InventoryUnit`  DECIMAL(32,6) NOT NULL,
	`RequirementQuantity_PurchaseUnit`  DECIMAL(38,6) NOT NULL,
	`RequirementQuantity_SalesUnit`  DECIMAL(38,6) NOT NULL,
	`PurchaseQuantity_InventoryUnit`  DECIMAL(38,6) NOT NULL,
	`PurchaseQuantity_PurchaseUnit`  DECIMAL(32,6) NOT NULL,
	`PurchaseQuantity_SalesUnit`  DECIMAL(38,6) NOT NULL
)
;
