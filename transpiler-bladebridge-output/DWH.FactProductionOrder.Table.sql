/****** Object:  Table [DWH].[FactProductionOrder]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DWH`.`FactProductionOrder`(
	`FactProductionOrderId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`DimProductionOrderId` int NOT NULL,
	`DimCompanyId` int NOT NULL,
	`DimProductId` int NOT NULL,
	`DimRouteId` int NOT NULL,
	`DimResourceId` int NOT NULL,
	`DimProductConfigurationId` int NOT NULL,
	`DimAccountingCurrencyId` int NOT NULL,
	`DimReportingCurrencyId` int NOT NULL,
	`DimGroupCurrencyId` int NOT NULL,
	`DimJournalPostingDateId` int NOT NULL,
	`DimScheduledProductionStartDateId` int NOT NULL,
	`DimScheduledProductionEndDateId` int NOT NULL,
	`DimProductionStartDateId` int NOT NULL,
	`DimProductionEndDateId` int NOT NULL,
	`ActualDeliveryDate` TIMESTAMP NOT NULL,
	`RequestedDeliveryDate` TIMESTAMP NOT NULL,
	`ScheduledDeliveryDate` TIMESTAMP NOT NULL,
	`DimOperationsId` int NOT NULL,
	`ProductionResourceCode`  STRING NOT NULL,
	`ProductionOrderStatus`  STRING NOT NULL,
	`CalculationType`  STRING NOT NULL,
	`InventDimCode`  STRING NOT NULL,
	`OTIF`  DECIMAL(38,7) NOT NULL,
	`InventUnit`  STRING NOT NULL,
	`EstimatedCostAmountAC`  DECIMAL(38,6) NOT NULL,
	`EstimatedCostAmountRC`  DECIMAL(38,6) NOT NULL,
	`EstimatedCostAmountGC`  DECIMAL(38,6) NOT NULL,
	`EstimatedCostAmountRC_Budget`  DECIMAL(38,6) NOT NULL,
	`EstimatedCostAmountGC_Budget`  DECIMAL(38,6) NOT NULL,
	`RealCostAmountAC`  DECIMAL(38,6) NOT NULL,
	`RealCostAmountRC`  DECIMAL(38,6) NOT NULL,
	`RealCostAmountGC`  DECIMAL(38,6) NOT NULL,
	`RealCostAmountRC_Budget`  DECIMAL(38,6) NOT NULL,
	`RealCostAmountGC_Budget`  DECIMAL(38,6) NOT NULL,
	`TotalScrapAmountAC`  DECIMAL(38,6) NOT NULL,
	`TotalScrapAmountRC`  DECIMAL(38,6) NOT NULL,
	`TotalScrapAmountGC`  DECIMAL(38,6) NOT NULL,
	`TotalScrapAmountRC_Budget`  DECIMAL(38,6) NOT NULL,
	`TotalScrapAmountGC_Budget`  DECIMAL(38,6) NOT NULL,
	`NetScrapAmountAC`  DECIMAL(38,6) NOT NULL,
	`NetScrapAmountRC`  DECIMAL(38,6) NOT NULL,
	`NetScrapAmountGC`  DECIMAL(38,6) NOT NULL,
	`NetScrapAmountRC_Budget`  DECIMAL(38,6) NOT NULL,
	`NetScrapAmountGC_Budget`  DECIMAL(38,6) NOT NULL,
	`AppliedExchangeRateAC`  DECIMAL(38,20) NOT NULL,
	`AppliedExchangeRateRC`  DECIMAL(38,20) NOT NULL,
	`AppliedExchangeRateGC`  DECIMAL(38,20) NOT NULL,
	`AppliedExchangeRateRC_Budget`  DECIMAL(38,20) NOT NULL,
	`AppliedExchangeRateGC_Budget`  DECIMAL(38,20) NOT NULL,
	`ProducedQuantity_InventoryUnit`  DECIMAL(38,6) ,
	`ProducedQuantity_PurchaseUnit`  DECIMAL(38,6) ,
	`ProducedQuantity_SalesUnit`  DECIMAL(38,6) ,
	`EstimatedQuantity_InventoryUnit`  DECIMAL(38,6) ,
	`EstimatedQuantity_PurchaseUnit`  DECIMAL(38,6) ,
	`OriginalEstimatedQuantity_SalesUnit`  DECIMAL(38,6) ,
	`RealConsumptionQuantity_InventoryUnit`  DECIMAL(38,6) ,
	`RealConsumptionQuantity_PurchaseUnit`  DECIMAL(38,6) ,
	`RealConsumptionQuantity_SalesUnit`  DECIMAL(38,6) ,
	`TotalScrapQuantity_InventoryUnit`  DECIMAL(38,6) ,
	`TotalScrapQuantity_PurchaseUnit`  DECIMAL(38,6) ,
	`TotalScrapQuantity_SalesUnit`  DECIMAL(38,6) ,
	`NetScrapQuantity_InventoryUnit`  DECIMAL(38,6) ,
	`NetScrapQuantity_PurchaseUnit`  DECIMAL(38,6) ,
	`NetScrapQuantity_SalesUnit`  DECIMAL(38,6) ,
	`EstimatedQuantity_SalesUnit`  DECIMAL(38,6) ,
	`EstimatedQuantityDetail_InventoryUnit`  DECIMAL(38,6) ,
	`EstimatedQuantityDetail_PurchaseUnit`  DECIMAL(38,6) ,
	`EstimatedQuantityDetail_SalesUnit`  DECIMAL(38,6) ,
	`NetEstimatedConsumptionQuantity_InventoryUnit`  DECIMAL(38,6) ,
	`NetEstimatedConsumptionQuantity_PurchaseUnit`  DECIMAL(38,6) ,
	`NetEstimatedConsumptionQuantity_SalesUnit`  DECIMAL(38,6) ,
	`OriginalEstimatedQuantity_InventoryUnit`  DECIMAL(38,6) ,
	`OriginalEstimatedQuantity_PurchaseUnit`  DECIMAL(38,6) ,
	`OriginalEstimatedQuantityDetail_InventoryUnit`  DECIMAL(38,6) ,
	`OriginalEstimatedQuantityDetail_PurchaseUnit`  DECIMAL(38,6) ,
	`OriginalEstimatedQuantityDetail_SalesUnit`  DECIMAL(38,6) ,
	`ProducedQuantityDetail_InventoryUnit`  DECIMAL(38,6) ,
	`ProducedQuantityDetail_PurchaseUnit`  DECIMAL(38,6) ,
	`ProducedQuantityDetail_SalesUnit`  DECIMAL(38,6) ,
	`ReportRemainderAsFinished_InventoryUnit`  DECIMAL(38,6) ,
	`ReportRemainderAsFinished_PurchaseUnit`  DECIMAL(38,6) ,
	`ReportRemainderAsFinished_SalesUnit`  DECIMAL(38,6) ,
	`ReportRemainderAsFinishedDetail_InventoryUnit`  DECIMAL(38,6) ,
	`ReportRemainderAsFinishedDetail_PurchaseUnit`  DECIMAL(38,6) ,
	`ReportRemainderAsFinishedDetail_SalesUnit`  DECIMAL(38,6) ,
	`TotalEstimatedConsumptionQuantity_InventoryUnit`  DECIMAL(38,6) ,
	`TotalEstimatedConsumptionQuantity_PurchaseUnit`  DECIMAL(38,6) ,
	`TotalEstimatedConsumptionQuantity_SalesUnit`  DECIMAL(38,6) ,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_FactProductionOrder` PRIMARY KEY CLUSTERED 
(
	`FactProductionOrderId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactProductionOrder_DimAccountingCurrencyId` FOREIGN KEY(`DimAccountingCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactProductionOrder_DimGroupCurrencyId` FOREIGN KEY(`DimGroupCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_DWH_FactProductionOrder_DimOperations` FOREIGN KEY(`DimOperationsId`)
REFERENCES `DWH`.`DimOperations` (`DimOperationsId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactProductionOrder_DimReportingCurrencyId` FOREIGN KEY(`DimReportingCurrencyId`)
REFERENCES `DWH`.`DimCurrency` (`DimCurrencyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactProductionOrder_DimCompany` FOREIGN KEY(`DimCompanyId`)
REFERENCES `DWH`.`DimCompany` (`DimCompanyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactProductionOrder_DimJournalPostingDateId` FOREIGN KEY(`DimJournalPostingDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactProductionOrder_DimProduct` FOREIGN KEY(`DimProductId`)
REFERENCES `DWH`.`DimProduct` (`DimProductId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactProductionOrder_DimProductConfiguration` FOREIGN KEY(`DimProductConfigurationId`)
REFERENCES `DWH`.`DimProductConfiguration` (`DimProductConfigurationId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactProductionOrder_DimProductionEndDateId` FOREIGN KEY(`DimProductionEndDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactProductionOrder_DimProductionOrder` FOREIGN KEY(`DimProductionOrderId`)
REFERENCES `DWH`.`DimProductionOrder` (`DimProductionOrderId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactProductionOrder_DimProductionStartDateId` FOREIGN KEY(`DimProductionStartDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactProductionOrder_DimResource` FOREIGN KEY(`DimResourceId`)
REFERENCES `DWH`.`DimResource` (`DimResourceId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactProductionOrder_DimRoute` FOREIGN KEY(`DimRouteId`)
REFERENCES `DWH`.`DimRoute` (`DimRouteId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactProductionOrder_DimScheduledProductionEndDateId` FOREIGN KEY(`DimScheduledProductionEndDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactProductionOrder_DimScheduledProductionStartDateId` FOREIGN KEY(`DimScheduledProductionStartDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;
