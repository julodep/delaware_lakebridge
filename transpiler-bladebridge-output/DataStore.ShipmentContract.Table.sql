/****** Object:  Table [DataStore].[ShipmentContract]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DataStore`.`ShipmentContract`(
	`ShipmentContractId` bigint NOT NULL,
	`ShipmentContractCode`  STRING NOT NULL,
	`ShipmentContractName`  STRING NOT NULL,
	`ShipmentContractCodeName`  STRING NOT NULL,
	`DimensionName`  STRING NOT NULL
)
;
