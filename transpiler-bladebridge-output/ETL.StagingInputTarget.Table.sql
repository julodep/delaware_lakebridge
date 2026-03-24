/****** Object:  Table [ETL].[StagingInputTarget]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `ETL`.`StagingInputTarget`(
	`TargetName`  STRING NOT NULL,
	`TargetSchema`  STRING NOT NULL,
	`TargetTable`  STRING NOT NULL,
	`SourceSchema`  STRING NOT NULL,
	`SourceTable`  STRING NOT NULL,
	`PrimaryKey`  STRING,
	`ActionType`  STRING NOT NULL,
	`Category`  STRING,
	`Status`  STRING NOT NULL,
	`Description`  STRING NOT NULL,
 CONSTRAINT `PK_ETL_StagingInputTarget` PRIMARY KEY CLUSTERED 
(
	`TargetName` ASC,
	`TargetSchema` ASC,
	`TargetTable` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
