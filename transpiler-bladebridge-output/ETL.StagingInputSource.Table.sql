/****** Object:  Table [ETL].[StagingInputSource]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `ETL`.`StagingInputSource`(
	`SourceName`  STRING NOT NULL,
	`Prefix`  STRING NOT NULL,
	`TargetSchema`  STRING NOT NULL,
	`SourceType`  STRING NOT NULL,
 CONSTRAINT `PK_ETL_StagingInputSource` PRIMARY KEY CLUSTERED 
(
	`SourceName` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
