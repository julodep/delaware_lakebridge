/****** Object:  Table [DataStore].[SalesInvoice]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore`.`SalesInvoice`(
	`SalesInvoiceCode`  STRING NOT NULL,
	`TransactionType`  STRING,
	`SalesInvoiceLineNumber`  DECIMAL(32,16) NOT NULL,
	`SalesInvoiceLineNumberCombination`  STRING,
	`HeaderRecId` bigint NOT NULL,
	`LineRecId` bigint NOT NULL,
	`SalesOrderCode`  STRING NOT NULL,
	`InventTransCode`  STRING NOT NULL,
	`InventDimCode`  STRING NOT NULL,
	`TaxWriteCode` INT,
	`SalesOrderStatus`  STRING,
	`CompanyCode`  STRING,
	`ProductCode`  STRING NOT NULL,
	`OrderCustomerCode`  STRING NOT NULL,
	`CustomerCode`  STRING NOT NULL,
	`DeliveryModeCode`  STRING NOT NULL,
	`PaymentTermsCode`  STRING NOT NULL,
	`DeliveryTermsCode`  STRING NOT NULL,
	`TransactionCurrencyCode`  STRING,
	`LedgerCode`  STRING NOT NULL,
	`OrigSalesOrderId`  STRING NOT NULL,
	`InvoiceDate` TIMESTAMP ,
	`RequestedDeliveryDate` TIMESTAMP ,
	`ConfirmedDeliveryDate` TIMESTAMP ,
	`SalesUnit`  STRING NOT NULL,
	`InvoicedQuantity`  DECIMAL(32,6) NOT NULL,
	`SalesPricePerUnitTC`  DECIMAL(38,6) ,
	`GrossSalesTC`  DECIMAL(38,6) ,
	`DiscountAmountTC`  DECIMAL(38,6) ,
	`InvoicedSalesAmountTC`  DECIMAL(38,6) ,
	`MarkupAmountTC`  DECIMAL(38,6) ,
	`NetSalesTC`  DECIMAL(38,6) 
)
;
