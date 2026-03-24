/****** Object:  Table [DWH].[DimOperations]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`DimOperations`(
	`DimOperationsId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`OperationCode`  STRING NOT NULL,
	`OperationName`  STRING NOT NULL,
	`OperationNumber` int NOT NULL,
	`OperationNumberNext` int NOT NULL,
	`OperationPriority` int NOT NULL,
	`OperationSequence` bigint NOT NULL,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_DimOperations` PRIMARY KEY CLUSTERED 
(
	`DimOperationsId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
