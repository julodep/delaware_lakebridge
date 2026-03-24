/****** Object:  Table [ETL].[ProjectParameters]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `ETL`.`ProjectParameters`(
	`EnvironmentName`  STRING NOT NULL,
	`ConfigurationFilter`  STRING NOT NULL,
	`ConfiguredValue`  STRING NOT NULL,
 CONSTRAINT `PK_ETL_ProjectParameters` PRIMARY KEY CLUSTERED 
(
	`EnvironmentName` ASC,
	`ConfigurationFilter` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
