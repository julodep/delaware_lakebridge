/****** Object:  Table [ETL].[StagingInputColumn]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `ETL`.`StagingInputColumn`(
	`SourceName`  STRING NOT NULL,
	`SourceSchema`  STRING NOT NULL,
	`SourceTable`  STRING NOT NULL,
	`SourceColumn`  STRING NOT NULL,
	`TargetColumn`  STRING NOT NULL,
	`IsNullable` `BOOLEAN` ,
	`Upper`  STRING,
	`Trim`  STRING,
	`SourceDataType`  STRING,
	`TargetDataType`  STRING,
	`ReplaceEmptyValue`  STRING,
	`DefaultValue`  STRING,
	`Description`  STRING NOT NULL,
	`Example`  STRING,
 CONSTRAINT `PK_ETL_StagingInputColumn` PRIMARY KEY CLUSTERED 
(
	`SourceName` ASC,
	`SourceSchema` ASC,
	`SourceTable` ASC,
	`SourceColumn` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
