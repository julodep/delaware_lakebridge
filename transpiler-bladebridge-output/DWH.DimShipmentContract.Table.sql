/****** Object:  Table [DWH].[DimShipmentContract]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`DimShipmentContract`(
	`DimShipmentContractId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`ShipmentContractId` bigint NOT NULL,
	`ShipmentContractCode`  STRING NOT NULL,
	`ShipmentContractName`  STRING NOT NULL,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_DimShipmentContract` PRIMARY KEY CLUSTERED 
(
	`DimShipmentContractId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
