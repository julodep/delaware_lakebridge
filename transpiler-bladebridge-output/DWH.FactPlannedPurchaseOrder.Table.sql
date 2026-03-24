/****** Object:  Table [DWH].[FactPlannedPurchaseOrder]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`FactPlannedPurchaseOrder`(
	`FactPlannedPurchaseOrderId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`DimProductId` int NOT NULL,
	`DimSupplierId` int NOT NULL,
	`DimCompanyId` int NOT NULL,
	`DimProductConfigurationId` int NOT NULL,
	`DimPurchaseOrderId` int NOT NULL,
	`DimRequirementDateId` int NOT NULL,
	`DimRequestedDateId` int NOT NULL,
	`DimOrderDateId` int NOT NULL,
	`DimDeliveryDateId` int NOT NULL,
	`PlannedPurchaseOrderCode`  STRING NOT NULL,
	`Status`  STRING NOT NULL,
	`LeadTime` int NOT NULL,
	`InventoryUnit`  STRING NOT NULL,
	`RequirementQuantity_InventoryUnit`  DECIMAL(38,6) ,
	`RequirementQuantity_PurchaseUnit`  DECIMAL(38,6) ,
	`RequirementQuantity_SalesUnit`  DECIMAL(38,6) ,
	`PurchaseUnit`  STRING NOT NULL,
	`PurchaseQuantity_InventoryUnit`  DECIMAL(38,6) ,
	`PurchaseQuantity_PurchaseUnit`  DECIMAL(38,6) ,
	`PurchaseQuantity_SalesUnit`  DECIMAL(38,6) ,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_FactPlannedPurchaseOrder` PRIMARY KEY CLUSTERED 
(
	`FactPlannedPurchaseOrderId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPlannedPurchaseOrder_DimCompany` FOREIGN KEY(`DimCompanyId`)
REFERENCES `DWH`.`DimCompany` (`DimCompanyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPlannedPurchaseOrder_DimDeliveryDateId` FOREIGN KEY(`DimDeliveryDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPlannedPurchaseOrder_DimOrderDateId` FOREIGN KEY(`DimOrderDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPlannedPurchaseOrder_DimProduct` FOREIGN KEY(`DimProductId`)
REFERENCES `DWH`.`DimProduct` (`DimProductId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPlannedPurchaseOrder_DimProductConfiguration` FOREIGN KEY(`DimProductConfigurationId`)
REFERENCES `DWH`.`DimProductConfiguration` (`DimProductConfigurationId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPlannedPurchaseOrder_DimPurchaseOrderId` FOREIGN KEY(`DimPurchaseOrderId`)
REFERENCES `DWH`.`DimPurchaseOrder` (`DimPurchaseOrderId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPlannedPurchaseOrder_DimRequestedDateId` FOREIGN KEY(`DimRequestedDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPlannedPurchaseOrder_DimRequirementDateId` FOREIGN KEY(`DimRequirementDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactPlannedPurchaseOrder_DimSupplier` FOREIGN KEY(`DimSupplierId`)
REFERENCES `DWH`.`DimSupplier` (`DimSupplierId`)
;
