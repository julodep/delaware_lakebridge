/****** Object:  Table [DWH].[DimLocalAccount]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`DimLocalAccount`(
	`DimLocalAccountId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`LocalAccountId` bigint NOT NULL,
	`LocalAccountCode`  STRING NOT NULL,
	`LocalAccountName`  STRING NOT NULL,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_DimLocalAccount` PRIMARY KEY CLUSTERED 
(
	`DimLocalAccountId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
