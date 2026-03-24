/****** Object:  Table [DataStore].[Operations]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore`.`Operations`(
	`OperationCode`  STRING NOT NULL,
	`OperationName`  STRING NOT NULL,
	`OperationSequence` BIGINT,
	`OperationNumber`  STRING NOT NULL,
	`OperationNumberNext` int NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`OperationPriority` int NOT NULL
)
;
