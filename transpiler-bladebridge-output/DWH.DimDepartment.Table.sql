/****** Object:  Table [DWH].[DimDepartment]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`DimDepartment`(
	`DimDepartmentId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`DepartmentId` bigint NOT NULL,
	`DepartmentCode`  STRING NOT NULL,
	`DepartmentName`  STRING NOT NULL,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_DimDepartment` PRIMARY KEY CLUSTERED 
(
	`DimDepartmentId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
