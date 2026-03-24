/****** Object:  Table [DataStore].[Markup]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore`.`Markup`(
	`CompanyCode`  STRING,
	`TransRecId` bigint NOT NULL,
	`MarkupCategory` int NOT NULL,
	`TransTableCode` int NOT NULL,
	`SurchargeTransport`  DECIMAL(32,6) NOT NULL,
	`SurchargePurchase`  DECIMAL(32,6) NOT NULL,
	`SurchargeDelivery`  DECIMAL(32,6) NOT NULL,
	`SurchargeTotal`  DECIMAL(34,6) 
)
;
