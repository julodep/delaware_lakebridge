/****** Object:  Table [DataStore3].[ProductivityTarget]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DataStore3`.`ProductivityTarget`(
	`ApplicableDate` TIMESTAMP ,
	`CompanyId`  STRING,
	`ResourceId`  STRING,
	`RouteId`  STRING,
	`ResourceTargetSpeed`  DECIMAL(32,17) 
)
;
