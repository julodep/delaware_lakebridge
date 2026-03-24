/****** Object:  Table [DWH].[DimIntercompany]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`DimIntercompany`(
	`DimIntercompanyId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`IntercompanyId` bigint NOT NULL,
	`IntercompanyCode`  STRING NOT NULL,
	`IntercompanyName`  STRING NOT NULL,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_DimIntercompany` PRIMARY KEY CLUSTERED 
(
	`DimIntercompanyId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
