/****** Object:  Table [DWH].[FactPurchaseOrder]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`FactPurchaseOrder`(
	`FactPurchaseOrderId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`DimPurchaseOrderId` int NOT NULL,
	`DimProductConfigurationId` int NOT NULL,
	`DimTransactionCurrencyId` int NOT NULL,
	`DimAccountingCurrencyId` int NOT NULL,
	`DimGroupCurrencyId` int NOT NULL,
	`DimReportingCurrencyId` int NOT NULL,
	`DimDeliveryTermsId` int NOT NULL,
	`DimDeliveryModeId` int NOT NULL,
	`DimPaymentTermsId` int NOT NULL,
	`DimProductId` int NOT NULL,
	`DimSupplierId` int NOT NULL,
	`DimOrderSupplierId` int NOT NULL,
	`DimCreationDateId` int NOT NULL,
	`DimRequestedDeliveryDateId` int NOT NULL,
	`DimConfirmedDeliveryDateId` int NOT NULL,
	`DimCompanyId` int NOT NULL,
	`PurchaseOrderLineNumber`  STRING NOT NULL,
	`OrderLineNumberCombination`  STRING NOT NULL,
	`DeliveryAddress`  STRING NOT NULL,
	`InventTransCode`  STRING NOT NULL,
	`PurchaseOrderStatus`  STRING NOT NULL,
	`PurchaseUnit`  STRING NOT NULL,
	`OrderedQuantity_InventoryUnit`  DECIMAL(38,6) ,
	`OrderedQuantity_PurchaseUnit`  DECIMAL(38,6) ,
	`OrderedQuantity_SalesUnit`  DECIMAL(38,6) ,
	`DeliveredQuantity_InventoryUnit`  DECIMAL(38,6) NOT NULL,
	`DeliveredQuantity_PurchaseUnit`  DECIMAL(38,6) ,
	`DeliveredQuantity_SalesUnit`  DECIMAL(38,6) NOT NULL,
	`PurchasePricePerUnitTC`  DECIMAL(38,6) NOT NULL,
	`PurchasePricePerUnitAC`  DECIMAL(38,6) NOT NULL,
	`PurchasePricePerUnitRC`  DECIMAL(38,6) NOT NULL,
	`PurchasePricePerUnitGC`  DECIMAL(38,6) NOT NULL,
	`PurchasePricePerUnitAC_Budget`  DECIMAL(38,6) NOT NULL,
	`PurchasePricePerUnitRC_Budget`  DECIMAL(38,6) NOT NULL,
	`PurchasePricePerUnitGC_Budget`  DECIMAL(38,6) NOT NULL,
	`GrossPurchaseTC`  DECIMAL(38,6) NOT NULL,
	`GrossPurchaseAC`  DECIMAL(38,6) NOT NULL,
	`GrossPurchaseRC`  DECIMAL(38,6) NOT NULL,
	`GrossPurchaseGC`  DECIMAL(38,6) NOT NULL,
	`GrossPurchaseAC_Budget`  DECIMAL(38,6) NOT NULL,
	`GrossPurchaseRC_Budget`  DECIMAL(38,6) NOT NULL,
	`GrossPurchaseGC_Budget`  DECIMAL(38,6) NOT NULL,
	`DiscountAmountTC`  DECIMAL(38,6) NOT NULL,
	`DiscountAmountAC`  DECIMAL(38,6) NOT NULL,
	`DiscountAmountRC`  DECIMAL(38,6) NOT NULL,
	`DiscountAmountGC`  DECIMAL(38,6) NOT NULL,
	`DiscountAmountAC_Budget`  DECIMAL(38,6) NOT NULL,
	`DiscountAmountRC_Budget`  DECIMAL(38,6) NOT NULL,
	`DiscountAmountGC_Budget`  DECIMAL(38,6) NOT NULL,
	`InvoicedPurchaseAmountTC`  DECIMAL(38,6) NOT NULL,
	`InvoicedPurchaseAmountAC`  DECIMAL(38,6) NOT NULL,
	`InvoicedPurchaseAmountRC`  DECIMAL(38,6) NOT NULL,
	`InvoicedPurchaseAmountGC`  DECIMAL(38,6) NOT NULL,
	`InvoicedPurchaseAmountAC_Budget`  DECIMAL(38,6) NOT NULL,
	`InvoicedPurchaseAmountRC_Budget`  DECIMAL(38,6) NOT NULL,
	`InvoicedPurchaseAmountGC_Budget`  DECIMAL(38,6) NOT NULL,
	`MarkupAmountTC`  DECIMAL(38,6) NOT NULL,
	`MarkupAmountAC`  DECIMAL(38,6) NOT NULL,
	`MarkupAmountRC`  DECIMAL(38,6) NOT NULL,
	`MarkupAmountGC`  DECIMAL(38,6) NOT NULL,
	`MarkupAmountAC_Budget`  DECIMAL(38,6) NOT NULL,
	`MarkupAmountRC_Budget`  DECIMAL(38,6) NOT NULL,
	`MarkupAmountGC_Budget`  DECIMAL(38,6) NOT NULL,
	`NetPurchaseAmountTC`  DECIMAL(38,6) NOT NULL,
	`NetPurchaseAmountAC`  DECIMAL(38,6) NOT NULL,
	`NetPurchaseAmountRC`  DECIMAL(38,6) NOT NULL,
	`NetPurchaseAmountGC`  DECIMAL(38,6) NOT NULL,
	`NetPurchaseAmountAC_Budget`  DECIMAL(38,6) NOT NULL,
	`NetPurchaseAmountRC_Budget`  DECIMAL(38,6) NOT NULL,
	`NetPurchaseAmountGC_Budget`  DECIMAL(38,6) NOT NULL,
	`SurchargeTransportTC`  DECIMAL(38,6) NOT NULL,
	`SurchargeTransportAC`  DECIMAL(38,6) NOT NULL,
	`SurchargeTransportRC`  DECIMAL(38,6) NOT NULL,
	`SurchargeTransportGC`  DECIMAL(38,6) NOT NULL,
	`SurchargeTransportAC_Budget`  DECIMAL(38,6) NOT NULL,
	`SurchargeTransportRC_Budget`  DECIMAL(38,6) NOT NULL,
	`SurchargeTransportGC_Budget`  DECIMAL(38,6) NOT NULL,
	`SurchargePurchaseTC`  DECIMAL(38,6) NOT NULL,
	`SurchargePurchaseAC`  DECIMAL(38,6) NOT NULL,
	`SurchargePurchaseRC`  DECIMAL(38,6) NOT NULL,
	`SurchargePurchaseGC`  DECIMAL(38,6) NOT NULL,
	`SurchargePurchaseAC_Budget`  DECIMAL(38,6) NOT NULL,
	`SurchargePurchaseRC_Budget`  DECIMAL(38,6) NOT NULL,
	`SurchargePurchaseGC_Budget`  DECIMAL(38,6) NOT NULL,
	`SurchargeDeliveryTC`  DECIMAL(38,6) NOT NULL,
	`SurchargeDeliveryAC`  DECIMAL(38,6) NOT NULL,
	`SurchargeDeliveryRC`  DECIMAL(38,6) NOT NULL,
	`SurchargeDeliveryGC`  DECIMAL(38,6) NOT NULL,
	`SurchargeDeliveryAC_Budget`  DECIMAL(38,6) NOT NULL,
	`SurchargeDeliveryRC_Budget`  DECIMAL(38,6) NOT NULL,
	`SurchargeDeliveryGC_Budget`  DECIMAL(38,6) NOT NULL,
	`SurchargeTotalTC`  DECIMAL(38,6) NOT NULL,
	`SurchargeTotalAC`  DECIMAL(38,6) NOT NULL,
	`SurchargeTotalRC`  DECIMAL(38,6) NOT NULL,
	`SurchargeTotalGC`  DECIMAL(38,6) NOT NULL,
	`SurchargeTotalAC_Budget`  DECIMAL(38,6) NOT NULL,
	`SurchargeTotalRC_Budget`  DECIMAL(38,6) NOT NULL,
	`SurchargeTotalGC_Budget`  DECIMAL(38,6) NOT NULL,
	`AppliedExchangeRateTC`  DECIMAL(38,6) NOT NULL,
	`AppliedExchangeRateAC`  DECIMAL(38,20) NOT NULL,
	`AppliedExchangeRateRC`  DECIMAL(38,20) NOT NULL,
	`AppliedExchangeRateGC`  DECIMAL(38,20) NOT NULL,
	`AppliedExchangeRateAC_Budget`  DECIMAL(38,20) NOT NULL,
	`AppliedExchangeRateRC_Budget`  DECIMAL(38,20) NOT NULL,
	`AppliedExchangeRateGC_Budget`  DECIMAL(38,20) NOT NULL,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_FactPurchaseOrder` PRIMARY KEY CLUSTERED 
(
	`FactPurchaseOrderId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurchaseOrder_DimAccountingCurrencyId` FOREIGN KEY(`DimAccountingCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurchaseOrder_DimCompany` FOREIGN KEY(`DimCompanyId`)
REFERENCES `DWH`.`DimCompany` (`DimCompanyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurchaseOrder_DimConfirmedDeliveryDateId` FOREIGN KEY(`DimConfirmedDeliveryDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurchaseOrder_DimCreationDateId` FOREIGN KEY(`DimCreationDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurchaseOrder_DimDeliveryMode` FOREIGN KEY(`DimDeliveryModeId`)
REFERENCES `DWH`.`DimDeliveryMode` (`DimDeliveryModeId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurchaseOrder_DimDeliveryTerms` FOREIGN KEY(`DimDeliveryTermsId`)
REFERENCES `DWH`.`DimDeliveryTerms` (`DimDeliveryTermsId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_FactPurchaseOrder_DimGroupCurrencyId` FOREIGN KEY(`DimGroupCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurchaseOrder_DimOrderSupplier` FOREIGN KEY(`DimOrderSupplierId`)
REFERENCES `DWH`.`DimSupplier` (`DimSupplierId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurchaseOrder_DimPaymentTerms` FOREIGN KEY(`DimPaymentTermsId`)
REFERENCES `DWH`.`DimPaymentTerms` (`DimPaymentTermsId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurchaseOrder_DimProduct` FOREIGN KEY(`DimProductId`)
REFERENCES `DWH`.`DimProduct` (`DimProductId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurchaseOrder_DimProductConfiguration` FOREIGN KEY(`DimProductConfigurationId`)
REFERENCES `DWH`.`DimProductConfiguration` (`DimProductConfigurationId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurchaseOrder_DimPurchaseOrderId` FOREIGN KEY(`DimPurchaseOrderId`)
REFERENCES `DWH`.`DimPurchaseOrder` (`DimPurchaseOrderId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_FactPurchaseOrder_DimReportingCurrencyId` FOREIGN KEY(`DimReportingCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurchaseOrder_DimRequestedDeliveryDateId` FOREIGN KEY(`DimRequestedDeliveryDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurchaseOrder_DimSupplier` FOREIGN KEY(`DimSupplierId`)
REFERENCES `DWH`.`DimSupplier` (`DimSupplierId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPurchaseOrder_DimTransactionCurrencyId` FOREIGN KEY(`DimTransactionCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;
