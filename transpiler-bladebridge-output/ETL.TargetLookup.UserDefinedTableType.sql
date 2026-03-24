/****** Object:  UserDefinedTableType [ETL].[TargetLookup]    Script Date: 03/03/2026 16:26:08 ******/
CREATE TYPE `ETL`.`TargetLookup` AS TABLE(
	`LookupTableAlias`  STRING NOT NULL,
	`LookupSchema`  STRING NOT NULL,
	`LookupTable`  STRING NOT NULL,
	`LookupColumn`  STRING NOT NULL,
	`TargetColumn`  STRING NULL,
	`SourceJoinColumns`  STRING NOT NULL,
	`LookupJoinColumns`  STRING NOT NULL
)
;
