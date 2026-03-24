/****** Object:  Table [ETL].[StagingInputIndex]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `ETL`.`StagingInputIndex`(
	`SourceName`  STRING NOT NULL,
	`TargetSchema`  STRING NOT NULL,
	`TargetTable`  STRING NOT NULL,
	`IndexName`  STRING NOT NULL,
	`IsClustered` `BOOLEAN` NOT NULL,
	`IsUnique` `BOOLEAN` NOT NULL,
	`Columns`  STRING NOT NULL,
	`IncludeColumns`  STRING,
	`FilteredPredicate`  STRING,
	`Description`  STRING,
 CONSTRAINT `PK_StagingInputIndex` PRIMARY KEY CLUSTERED 
(
	`IndexName` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
