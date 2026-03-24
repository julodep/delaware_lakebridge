/****** Object:  Table [DWH].[DimOutstandingPeriod]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DWH`.`DimOutstandingPeriod`(
	`DimOutstandingPeriodId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`OutstandingPeriodCode`  STRING NOT NULL,
	`OutstandingPeriod`  STRING NOT NULL,
	`SortOrder` int NOT NULL,
 CONSTRAINT `PK_DimOutstandingPeriod` PRIMARY KEY CLUSTERED 
(
	`DimOutstandingPeriodId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
