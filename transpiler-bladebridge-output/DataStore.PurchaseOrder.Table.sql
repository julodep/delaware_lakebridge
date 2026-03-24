/****** Object:  Table [DataStore].[PurchaseOrder]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DataStore`.`PurchaseOrder`(
	`PurchaseOrderCode`  STRING,
	`RecId` bigint NOT NULL,
	`PurchaseOrderLineNumber` bigint NOT NULL,
	`OrderLineNumberCombination`  STRING,
	`DeliveryAddress`  STRING NOT NULL,
	`CompanyCode`  STRING,
	`InventTransCode`  STRING NOT NULL,
	`ProductCode`  STRING NOT NULL,
	`SupplierCode`  STRING,
	`OrderSupplierCode`  STRING,
	`DeliveryModeCode`  STRING NOT NULL,
	`PaymentTermsCode`  STRING NOT NULL,
	`DeliveryTermsCode`  STRING NOT NULL,
	`PurchaseOrderStatus`  STRING NOT NULL,
	`TransactionCurrencyCode`  STRING,
	`InventDimCode`  STRING NOT NULL,
	`CreationDate` TIMESTAMP ,
	`RequestedDeliveryDate` TIMESTAMP NOT NULL,
	`ConfirmedDeliveryDate` TIMESTAMP NOT NULL,
	`OrderedQuantity`  DECIMAL(32,6) NOT NULL,
	`OrderedQuantityRemaining`  DECIMAL(32,6) NOT NULL,
	`DeliveredQuantity`  DECIMAL(33,6) ,
	`PurchaseUnit`  STRING NOT NULL,
	`PurchasePricePerUnitTC`  DECIMAL(38,6) NOT NULL,
	`GrossPurchaseTC`  DECIMAL(38,6) ,
	`DiscountAmountTC`  DECIMAL(38,6) ,
	`InvoicedPurchaseAmountTC`  DECIMAL(38,6) ,
	`MarkupAmountTC`  DECIMAL(38,6) ,
	`NetPurchaseTC`  DECIMAL(38,6) 
)
;
