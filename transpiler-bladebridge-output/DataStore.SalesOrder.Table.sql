/****** Object:  Table [DataStore].[SalesOrder]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore`.`SalesOrder`(
	`SalesOrderCode`  STRING NOT NULL,
	`SalesOrderLineNumber`  DECIMAL(32,16) NOT NULL,
	`SalesOrderLineNumberCombination`  STRING,
	`OrderTransaction`  STRING,
	`DeliveryAddress`  STRING NOT NULL,
	`DocumentStatus`  STRING NOT NULL,
	`HeaderRecId` bigint NOT NULL,
	`LineRecId` bigint NOT NULL,
	`DefaultDimension` bigint NOT NULL,
	`CompanyCode`  STRING,
	`InventTransCode`  STRING NOT NULL,
	`InventDimCode`  STRING NOT NULL,
	`OrderCustomerCode`  STRING NOT NULL,
	`CustomerCode`  STRING NOT NULL,
	`ProductCode`  STRING NOT NULL,
	`DeliveryModeCode`  STRING NOT NULL,
	`PaymentTermsCode`  STRING NOT NULL,
	`DeliveryTermsCode`  STRING NOT NULL,
	`SalesOrderStatus`  STRING NOT NULL,
	`TransactionCurrencyCode`  STRING,
	`CreationDate` TIMESTAMP ,
	`RequestedShippingDate` TIMESTAMP ,
	`ConfirmedShippingDate` TIMESTAMP ,
	`RequestedDeliveryDate` TIMESTAMP ,
	`ConfirmedDeliveryDate` TIMESTAMP ,
	`FirstShipmentDate` TIMESTAMP ,
	`LastShipmentDate` TIMESTAMP ,
	`SalesUnit`  STRING NOT NULL,
	`OrderedQuantity`  DECIMAL(32,6) NOT NULL,
	`OrderedQuantityRemaining`  DECIMAL(32,6) NOT NULL,
	`DeliveredQuantity`  DECIMAL(33,6) ,
	`SalesPricePerUnitTC`  DECIMAL(38,6) ,
	`GrossSalesTC`  DECIMAL(38,6) ,
	`DiscountAmountTC`  DECIMAL(38,6) ,
	`InvoicedSalesAmountTC`  DECIMAL(38,6) ,
	`MarkupAmountTC`  DECIMAL(38,6) ,
	`NetSalesTC`  DECIMAL(38,6) 
)
;
