/****** Object:  Table [DWH].[FactSalesOrder]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`FactSalesOrder`(
	`FactSalesOrderId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`DimSalesOrderId` int NOT NULL,
	`DimCompanyId` int NOT NULL,
	`DimDeliveryModeId` int NOT NULL,
	`DimDeliveryTermsId` int NOT NULL,
	`DimPaymentTermsId` int NOT NULL,
	`DimProductId` int NOT NULL,
	`DimProductConfigurationId` int NOT NULL,
	`DimTransactionCurrencyId` int NOT NULL,
	`DimAccountingCurrencyId` int NOT NULL,
	`DimReportingCurrencyId` int NOT NULL,
	`DimGroupCurrencyId` int NOT NULL,
	`DimCustomerId` int NOT NULL,
	`DimOrderCustomerId` int NOT NULL,
	`DimCreationDateId` int NOT NULL,
	`DimRequestedDeliveryDateId` int NOT NULL,
	`DimConfirmedDeliveryDateId` int NOT NULL,
	`DimRequestedShippingDateId` int NOT NULL,
	`DimConfirmedShippingDateId` int NOT NULL,
	`DimLastShipmentDateId` int NOT NULL,
	`SalesOrderStatus`  STRING NOT NULL,
	`DocumentStatus`  STRING NOT NULL,
	`SalesUnit`  STRING NOT NULL,
	`SalesOrderLineNumber`  STRING NOT NULL,
	`SalesOrderLineNumberCombination`  STRING NOT NULL,
	`DeliveryAddress`  STRING NOT NULL,
	`InventTransCode`  STRING NOT NULL,
	`InventDimCode`  STRING NOT NULL,
	`SalesPricePerUnitTC`  DECIMAL(38,6) ,
	`SalesPricePerUnitAC`  DECIMAL(38,6) ,
	`SalesPricePerUnitRC`  DECIMAL(38,6) ,
	`SalesPricePerUnitGC`  DECIMAL(38,6) ,
	`SalesPricePerUnitAC_Budget`  DECIMAL(38,6) ,
	`SalesPricePerUnitRC_Budget`  DECIMAL(38,6) ,
	`SalesPricePerUnitGC_Budget`  DECIMAL(38,6) ,
	`GrossSalesTC`  DECIMAL(38,6) ,
	`GrossSalesAC`  DECIMAL(38,6) ,
	`GrossSalesRC`  DECIMAL(38,6) ,
	`GrossSalesGC`  DECIMAL(38,6) ,
	`GrossSalesAC_Budget`  DECIMAL(38,6) ,
	`GrossSalesRC_Budget`  DECIMAL(38,6) ,
	`GrossSalesGC_Budget`  DECIMAL(38,6) ,
	`DiscountAmountTC`  DECIMAL(38,6) ,
	`DiscountAmountAC`  DECIMAL(38,6) ,
	`DiscountAmountRC`  DECIMAL(38,6) ,
	`DiscountAmountGC`  DECIMAL(38,6) ,
	`DiscountAmountAC_Budget`  DECIMAL(38,6) ,
	`DiscountAmountRC_Budget`  DECIMAL(38,6) ,
	`DiscountAmountGC_Budget`  DECIMAL(38,6) ,
	`InvoicedSalesAmountTC`  DECIMAL(38,17) ,
	`InvoicedSalesAmountAC`  DECIMAL(38,6) ,
	`InvoicedSalesAmountRC`  DECIMAL(38,6) ,
	`InvoicedSalesAmountGC`  DECIMAL(38,6) ,
	`InvoicedSalesAmountAC_Budget`  DECIMAL(38,6) ,
	`InvoicedSalesAmountRC_Budget`  DECIMAL(38,6) ,
	`InvoicedSalesAmountGC_Budget`  DECIMAL(38,6) ,
	`NetSalesAmountTC`  DECIMAL(38,6) ,
	`NetSalesAmountAC`  DECIMAL(38,6) ,
	`NetSalesAmountRC`  DECIMAL(38,6) ,
	`NetSalesAmountGC`  DECIMAL(38,6) ,
	`NetSalesAmountAC_Budget`  DECIMAL(38,6) ,
	`NetSalesAmountRC_Budget`  DECIMAL(38,6) ,
	`NetSalesAmountGC_Budget`  DECIMAL(38,6) ,
	`CostOfGoodsSoldTC`  DECIMAL(38,6) ,
	`CostOfGoodsSoldAC`  DECIMAL(38,6) ,
	`CostOfGoodsSoldRC`  DECIMAL(38,6) ,
	`CostOfGoodsSoldGC`  DECIMAL(38,6) ,
	`CostOfGoodsSoldAC_Budget`  DECIMAL(38,6) ,
	`CostOfGoodsSoldRC_Budget`  DECIMAL(38,6) ,
	`CostOfGoodsSoldGC_Budget`  DECIMAL(38,6) ,
	`GrossMarginTC`  DECIMAL(38,6) ,
	`GrossMarginAC`  DECIMAL(38,6) ,
	`GrossMarginRC`  DECIMAL(38,6) ,
	`GrossMarginGC`  DECIMAL(38,6) ,
	`GrossMarginAC_Budget`  DECIMAL(38,6) ,
	`GrossMarginRC_Budget`  DECIMAL(38,6) ,
	`GrossMarginGC_Budget`  DECIMAL(38,6) ,
	`AppliedExchangeRateTC`  DECIMAL(38,6) ,
	`AppliedExchangeRateAC`  DECIMAL(38,20) ,
	`AppliedExchangeRateRC`  DECIMAL(38,20) ,
	`AppliedExchangeRateGC`  DECIMAL(38,20) ,
	`AppliedExchangeRateAC_Budget`  DECIMAL(38,20) ,
	`AppliedExchangeRateRC_Budget`  DECIMAL(38,20) ,
	`AppliedExchangeRateGC_Budget`  DECIMAL(38,20) ,
	`OrderedQuantity_InventoryUnit`  DECIMAL(38,6) ,
	`OrderedQuantity_PurchaseUnit`  DECIMAL(38,6) ,
	`OrderedQuantity_SalesUnit`  DECIMAL(38,6) ,
	`DeliveredQuantity_InventoryUnit`  DECIMAL(38,6) ,
	`DeliveredQuantity_PurchaseUnit`  DECIMAL(38,6) ,
	`DeliveredQuantity_SalesUnit`  DECIMAL(38,6) ,
	`SurchargeDeliveryAC`  DECIMAL(38,6) ,
	`SurchargeDeliveryAC_Budget`  DECIMAL(38,6) ,
	`SurchargeDeliveryGC`  DECIMAL(38,6) ,
	`SurchargeDeliveryGC_Budget`  DECIMAL(38,6) ,
	`SurchargeDeliveryRC`  DECIMAL(38,6) ,
	`SurchargeDeliveryRC_Budget`  DECIMAL(38,6) ,
	`SurchargeDeliveryTC`  DECIMAL(38,6) ,
	`SurchargePurchaseAC`  DECIMAL(38,6) ,
	`SurchargePurchaseAC_Budget`  DECIMAL(38,6) ,
	`SurchargePurchaseGC`  DECIMAL(38,6) ,
	`SurchargePurchaseGC_Budget`  DECIMAL(38,6) ,
	`SurchargePurchaseRC`  DECIMAL(38,6) ,
	`SurchargePurchaseRC_Budget`  DECIMAL(38,6) ,
	`SurchargePurchaseTC`  DECIMAL(38,6) ,
	`SurchargeTotalAC`  DECIMAL(38,6) ,
	`SurchargeTotalAC_Budget`  DECIMAL(38,6) ,
	`SurchargeTotalGC`  DECIMAL(38,6) ,
	`SurchargeTotalGC_Budget`  DECIMAL(38,6) ,
	`SurchargeTotalRC`  DECIMAL(38,6) ,
	`SurchargeTotalRC_Budget`  DECIMAL(38,6) ,
	`SurchargeTotalTC`  DECIMAL(38,6) ,
	`SurchargeTransportAC`  DECIMAL(38,6) ,
	`SurchargeTransportAC_Budget`  DECIMAL(38,6) ,
	`SurchargeTransportGC`  DECIMAL(38,6) ,
	`SurchargeTransportGC_Budget`  DECIMAL(38,6) ,
	`SurchargeTransportRC`  DECIMAL(38,6) ,
	`SurchargeTransportRC_Budget`  DECIMAL(38,6) ,
	`SurchargeTransportTC`  DECIMAL(38,6) ,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_FactSalesOrder` PRIMARY KEY CLUSTERED 
(
	`FactSalesOrderId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactSalesOrder_DimAccountingCurrencyId` FOREIGN KEY(`DimAccountingCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactSalesOrder_DimCompanyId` FOREIGN KEY(`DimCompanyId`)
REFERENCES `DWH`.`DimCompany` (`DimCompanyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_DWH_FactSalesOrder_DimConfirmedDeliveryDateId` FOREIGN KEY(`DimConfirmedDeliveryDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH CHECK ADD  CONSTRAINT `FK_DWH_FactSalesOrder_DimConfirmedShippingDateId` FOREIGN KEY(`DimConfirmedShippingDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH CHECK ADD  CONSTRAINT `FK_DWH_FactSalesOrder_DimCreationDateId` FOREIGN KEY(`DimCreationDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactSalesOrder_DimCustomerId` FOREIGN KEY(`DimCustomerId`)
REFERENCES `DWH`.`DimCustomer` (`DimCustomerId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactSalesOrder_DimDeliveryModeId` FOREIGN KEY(`DimDeliveryModeId`)
REFERENCES `DWH`.`DimDeliveryMode` (`DimDeliveryModeId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactSalesOrder_DimDeliveryTermsId` FOREIGN KEY(`DimDeliveryTermsId`)
REFERENCES `DWH`.`DimDeliveryTerms` (`DimDeliveryTermsId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactSalesOrder_DimGroupCurrencyId` FOREIGN KEY(`DimGroupCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_DWH_FactSalesOrder_DimLastShipmentDateId` FOREIGN KEY(`DimLastShipmentDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactSalesOrder_DimOrderCustomerId` FOREIGN KEY(`DimOrderCustomerId`)
REFERENCES `DWH`.`DimCustomer` (`DimCustomerId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactSalesOrder_DimPaymentTermsId` FOREIGN KEY(`DimPaymentTermsId`)
REFERENCES `DWH`.`DimPaymentTerms` (`DimPaymentTermsId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactSalesOrder_DimProductConfigurationId` FOREIGN KEY(`DimProductConfigurationId`)
REFERENCES `DWH`.`DimProductConfiguration` (`DimProductConfigurationId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactSalesOrder_DimProductId` FOREIGN KEY(`DimProductId`)
REFERENCES `DWH`.`DimProduct` (`DimProductId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactSalesOrder_DimReportingCurrencyId` FOREIGN KEY(`DimReportingCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_DWH_FactSalesOrder_DimRequestedDeliveryDateId` FOREIGN KEY(`DimRequestedDeliveryDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH CHECK ADD  CONSTRAINT `FK_DWH_FactSalesOrder_DimRequestedShippingDateId` FOREIGN KEY(`DimRequestedShippingDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactSalesOrder_DimSalesOrderId` FOREIGN KEY(`DimSalesOrderId`)
REFERENCES `DWH`.`DimSalesOrder` (`DimSalesOrderId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactSalesOrder_DimTransactionCurrencyId` FOREIGN KEY(`DimTransactionCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;
