/****** Object:  Table [ETL].[StagingInputTable]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `ETL`.`StagingInputTable`(
	`SourceName`  STRING NOT NULL,
	`SourceSchema`  STRING NOT NULL,
	`SourceTable`  STRING NOT NULL,
	`TargetTable`  STRING NOT NULL,
	`PrimaryKey`  STRING,
	`WhereClause`  STRING,
	`DeltaWhereClause`  STRING,
	`Category`  STRING,
	`Status`  STRING NOT NULL,
	`Load` `BOOLEAN` NOT NULL,
	`Rebuild` `BOOLEAN` NOT NULL,
	`Description`  STRING NOT NULL,
 CONSTRAINT `PK_ETL_StagingInputTable` PRIMARY KEY CLUSTERED 
(
	`SourceName` ASC,
	`SourceSchema` ASC,
	`SourceTable` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF),
 CONSTRAINT `UC_ETL_StagingInputTable_SourceNameTargetTable` UNIQUE NONCLUSTERED 
(
	`SourceName` ASC,
	`TargetTable` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
