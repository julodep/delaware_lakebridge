/****** Object:  Table [DataStore].[PlannedPurchaseOrder]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore`.`PlannedPurchaseOrder`(
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
	`RequirementQuantity`  DECIMAL(32,6) NOT NULL,
	`PurchaseUnit`  STRING NOT NULL,
	`PurchaseQuantity`  DECIMAL(32,6) NOT NULL
)
;
