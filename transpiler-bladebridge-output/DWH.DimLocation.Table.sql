/****** Object:  Table [DWH].[DimLocation]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`DimLocation`(
	`DimLocationId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`LocationId` bigint NOT NULL,
	`LocationCode`  STRING NOT NULL,
	`LocationName`  STRING NOT NULL,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_DimLocation` PRIMARY KEY CLUSTERED 
(
	`DimLocationId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
