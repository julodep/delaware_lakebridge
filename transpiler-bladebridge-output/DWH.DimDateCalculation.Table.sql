/****** Object:  Table [DWH].[DimDateCalculation]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`DimDateCalculation`(
	`DateCalculation`  STRING NOT NULL,
 CONSTRAINT `PK_DimDateCalculation` PRIMARY KEY CLUSTERED 
(
	`DateCalculation` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
