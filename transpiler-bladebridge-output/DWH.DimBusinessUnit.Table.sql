/****** Object:  Table [DWH].[DimBusinessUnit]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`DimBusinessUnit`(
	`DimBusinessUnitId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`BusinessUnitId` bigint NOT NULL,
	`BusinessUnitCode`  STRING NOT NULL,
	`BusinessUnitName`  STRING NOT NULL,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_DimBusinessUnit` PRIMARY KEY CLUSTERED 
(
	`DimBusinessUnitId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
