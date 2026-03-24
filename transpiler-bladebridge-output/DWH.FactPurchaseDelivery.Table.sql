/****** Object:  Table [DWH].[FactPurchaseDelivery]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`FactPurchaseDelivery`(
	`FactPurchaseDeliveryId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`DimPurchaseOrderId` int NOT NULL,
	`DimCompanyId` int NOT NULL,
	`DimProductConfigurationId` int NOT NULL,
	`DimProductId` int NOT NULL,
	`DimDeliveryTermsId` int NOT NULL,
	`DimDeliveryModeId` int NOT NULL,
	`DimOrderSupplierId` int NOT NULL,
	`DimSupplierId` int NOT NULL,
	`DimActualDeliveryDateId` int NOT NULL,
	`DimRequestedDeliveryDateId` int NOT NULL,
	`DimConfirmedDeliveryDateId` int NOT NULL,
	`PackingSlipCode`  STRING NOT NULL,
	`PurchaseType`  STRING NOT NULL,
	`PurchaseOrderLineNumber` bigint NOT NULL,
	`DeliveryName`  STRING NOT NULL,
	`DeliveryLineNumber` int NOT NULL,
	`PurchaseUnit`  STRING NOT NULL,
	`OrderedQuantity_InventoryUnit`  DECIMAL(38,6) ,
	`OrderedQuantity_PurchaseUnit`  DECIMAL(38,6) ,
	`OrderedQuantity_SalesUnit`  DECIMAL(38,6) ,
	`DeliveredQuantity_InventoryUnit`  DECIMAL(38,6) ,
	`DeliveredQuantity_PurchaseUnit`  DECIMAL(38,6) ,
	`DeliveredQuantity_SalesUnit`  DECIMAL(38,6) ,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_FactDeliveries` PRIMARY KEY CLUSTERED 
(
	`FactPurchaseDeliveryId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;

WITH CHECK ADD  CONSTRAINT `FK_FactDeliveries_DimActualDeliveryDateId` FOREIGN KEY(`DimActualDeliveryDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactDeliveries_DimCompany` FOREIGN KEY(`DimCompanyId`)
REFERENCES `DWH`.`DimCompany` (`DimCompanyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactDeliveries_DimConfirmedDeliveryDateId` FOREIGN KEY(`DimConfirmedDeliveryDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactDeliveries_DimDeliveryMode` FOREIGN KEY(`DimDeliveryModeId`)
REFERENCES `DWH`.`DimDeliveryMode` (`DimDeliveryModeId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactDeliveries_DimDeliveryTerms` FOREIGN KEY(`DimDeliveryTermsId`)
REFERENCES `DWH`.`DimDeliveryTerms` (`DimDeliveryTermsId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactDeliveries_DimOrderSupplierId` FOREIGN KEY(`DimOrderSupplierId`)
REFERENCES `DWH`.`DimSupplier` (`DimSupplierId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactDeliveries_DimProduct` FOREIGN KEY(`DimProductId`)
REFERENCES `DWH`.`DimProduct` (`DimProductId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactDeliveries_DimProductConfiguration` FOREIGN KEY(`DimProductConfigurationId`)
REFERENCES `DWH`.`DimProductConfiguration` (`DimProductConfigurationId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactDeliveries_DimPurchaseOrderId` FOREIGN KEY(`DimPurchaseOrderId`)
REFERENCES `DWH`.`DimPurchaseOrder` (`DimPurchaseOrderId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactDeliveries_DimRequestedDeliveryDateId` FOREIGN KEY(`DimRequestedDeliveryDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactDeliveries_DimSupplierId` FOREIGN KEY(`DimSupplierId`)
REFERENCES `DWH`.`DimSupplier` (`DimSupplierId`)
;
