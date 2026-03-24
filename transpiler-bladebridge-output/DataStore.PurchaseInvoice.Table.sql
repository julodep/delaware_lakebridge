/****** Object:  Table [DataStore].[PurchaseInvoice]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore`.`PurchaseInvoice`(
	`PurchaseInvoiceCode`  STRING,
	`InternalInvoiceCode`  STRING NOT NULL,
	`TransactionType`  STRING,
	`PurchaseInvoiceLineNumber`  DECIMAL(32,16) NOT NULL,
	`InvoiceLineNumberCombination`  STRING,
	`LineDescription`  STRING NOT NULL,
	`LineRecId` bigint NOT NULL,
	`HeaderRecId` bigint NOT NULL,
	`CompanyCode`  STRING,
	`ProductCode`  STRING NOT NULL,
	`PurchaseOrderCode`  STRING NOT NULL,
	`InventTransCode`  STRING NOT NULL,
	`InventDimCode`  STRING NOT NULL,
	`TaxWriteCode` INT,
	`SupplierCode`  STRING,
	`DeliveryModeCode`  STRING,
	`PaymentTermsCode`  STRING,
	`DeliveryTermsCode`  STRING,
	`PurchaseOrderStatus`  STRING,
	`TransactionCurrencyCode`  STRING,
	`DefaultDimension` bigint NOT NULL,
	`InvoiceDate` TIMESTAMP NOT NULL,
	`PurchaseUnit`  STRING,
	`InvoicedQuantity`  DECIMAL(38,12) ,
	`PurchasePricePerUnitTC`  DECIMAL(38,6) ,
	`GrossPurchaseTC`  DECIMAL(38,6) ,
	`DiscountAmountTC`  DECIMAL(38,6) ,
	`InvoicedPurchaseAmountTC`  DECIMAL(38,6) ,
	`MarkupAmountTC`  DECIMAL(38,6) ,
	`NetPurchaseTC`  DECIMAL(38,6) ,
	`NetPurchaseInclTaxTC`  DECIMAL(38,6) 
)
;
