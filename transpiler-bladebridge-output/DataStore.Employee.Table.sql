/****** Object:  Table [DataStore].[Employee]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore`.`Employee`(
	`EmployeeId` bigint NOT NULL,
	`EmployeeCode`  STRING NOT NULL,
	`EmployeeName`  STRING NOT NULL,
	`EmployeeCodeName`  STRING NOT NULL,
	`DimensionName`  STRING NOT NULL
)
;
