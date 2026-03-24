/****** Object:  Table [DWH].[FactSalesShipment]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`FactSalesShipment`(
	`FactSalesShipmentId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`DimSalesOrderId` int NOT NULL,
	`DimCompanyId` int NOT NULL,
	`DimProductId` int NOT NULL,
	`DimProductConfigurationId` int NOT NULL,
	`DimCustomerId` int NOT NULL,
	`DimOrderCustomerId` INT,
	`DimRequestedShippingDateId` int NOT NULL,
	`DimConfirmedShippingDateId` int NOT NULL,
	`DimActualDeliveryDateId` int NOT NULL,
	`CustPackingSlipCode`  STRING NOT NULL,
	`CustPackingSlipLineNumber`  DECIMAL(32,17) NOT NULL,
	`CustPackingSlipLineNumberCombination`  STRING NOT NULL,
	`OrderedQuantity_InventoryUnit`  DECIMAL(38,6) ,
	`OrderedQuantity_PurchaseUnit`  DECIMAL(38,6) ,
	`OrderedQuantity_SalesUnit`  DECIMAL(38,6) ,
	`DeliveredQuantity_InventoryUnit`  DECIMAL(38,6) ,
	`DeliveredQuantity_PurchaseUnit`  DECIMAL(38,6) ,
	`DeliveredQuantity_SalesUnit`  DECIMAL(38,6) ,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_FactSalesShipment` PRIMARY KEY CLUSTERED 
(
	`FactSalesShipmentId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;

WITH CHECK ADD  CONSTRAINT `FK_DWH_FactSalesShipment_DimProductConfigurationId` FOREIGN KEY(`DimProductConfigurationId`)
REFERENCES `DWH`.`DimProductConfiguration` (`DimProductConfigurationId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactSalesShipment_DimActualDeliveryDateId` FOREIGN KEY(`DimActualDeliveryDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactSalesShipment_DimCompany` FOREIGN KEY(`DimCompanyId`)
REFERENCES `DWH`.`DimCompany` (`DimCompanyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactSalesShipment_DimConfirmedShippingDateId` FOREIGN KEY(`DimConfirmedShippingDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactSalesShipment_DimCustomer` FOREIGN KEY(`DimCustomerId`)
REFERENCES `DWH`.`DimCustomer` (`DimCustomerId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactSalesShipment_DimOrderCustomer` FOREIGN KEY(`DimOrderCustomerId`)
REFERENCES `DWH`.`DimCustomer` (`DimCustomerId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactSalesShipment_DimProduct` FOREIGN KEY(`DimProductId`)
REFERENCES `DWH`.`DimProduct` (`DimProductId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactSalesShipment_DimRequestedShippingDateId` FOREIGN KEY(`DimRequestedShippingDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH CHECK ADD  CONSTRAINT `FK_FactSalesShipment_DimSalesOrder` FOREIGN KEY(`DimSalesOrderId`)
REFERENCES `DWH`.`DimSalesOrder` (`DimSalesOrderId`)
;
