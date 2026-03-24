/****** Object:  Table [ETL].[StringMap]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `ETL`.`StringMap`(
	`SourceSystem`  STRING NOT NULL,
	`SourceTable`  STRING NOT NULL,
	`SourceColumn`  STRING NOT NULL,
	`Enum`  STRING NOT NULL,
	`Name`  STRING NOT NULL
)
;
