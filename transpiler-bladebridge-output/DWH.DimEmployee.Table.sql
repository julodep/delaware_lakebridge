/****** Object:  Table [DWH].[DimEmployee]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`DimEmployee`(
	`DimEmployeeId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`EmployeeId` bigint NOT NULL,
	`EmployeeCode`  STRING NOT NULL,
	`EmployeeName`  STRING NOT NULL,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_DimEmployee` PRIMARY KEY CLUSTERED 
(
	`DimEmployeeId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
