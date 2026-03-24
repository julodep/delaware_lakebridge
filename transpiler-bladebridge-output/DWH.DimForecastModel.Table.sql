/****** Object:  Table [DWH].[DimForecastModel]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`DimForecastModel`(
	`DimForeCastModelId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`ForecastModelCode`  STRING NOT NULL,
	`ForecastModelName`  STRING NOT NULL,
	`ForecastSubmodelCode`  STRING NOT NULL,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_DimForeCastModel` PRIMARY KEY CLUSTERED 
(
	`DimForeCastModelId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
