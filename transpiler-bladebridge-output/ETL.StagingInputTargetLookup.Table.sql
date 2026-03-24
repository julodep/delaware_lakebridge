/****** Object:  Table [ETL].[StagingInputTargetLookup]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `ETL`.`StagingInputTargetLookup`(
	`TargetName`  STRING NOT NULL,
	`TargetSchema`  STRING NOT NULL,
	`TargetTable`  STRING NOT NULL,
	`LookupTableAlias`  STRING NOT NULL,
	`LookupSchema`  STRING NOT NULL,
	`LookupTable`  STRING NOT NULL,
	`LookupColumn`  STRING NOT NULL,
	`TargetColumn`  STRING NOT NULL,
	`SourceJoinColumns`  STRING NOT NULL,
	`LookupJoinColumns`  STRING NOT NULL,
	`Description`  STRING,
 CONSTRAINT `PK_ETL_StagingInputTargetLookup` PRIMARY KEY CLUSTERED 
(
	`TargetName` ASC,
	`TargetSchema` ASC,
	`TargetTable` ASC,
	`LookupTableAlias` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
