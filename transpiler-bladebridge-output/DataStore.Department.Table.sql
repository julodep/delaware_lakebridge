/****** Object:  Table [DataStore].[Department]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DataStore`.`Department`(
	`DepartmentId` bigint NOT NULL,
	`DepartmentCode`  STRING NOT NULL,
	`DepartmentName`  STRING NOT NULL,
	`DepartmentCodeName`  STRING NOT NULL,
	`DimensionName`  STRING NOT NULL
)
;
