/****** Object:  Table [ETL].[Screening]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `ETL`.`Screening`(
	`ScreeningId` int NOT NULL,
	`ScreeningCategory`  STRING NOT NULL,
	`ScreeningOrder` int NOT NULL,
	`ScreeningDescription`  STRING,
	`ScreeningActive` `BOOLEAN` NOT NULL,
	`TableName`  STRING NOT NULL,
	`ColumnName`  STRING NOT NULL,
	`ReferenceTable`  STRING,
	`ReferenceColumns`  STRING,
	`CorrectionValue`  STRING,
	`CompanyCode`  STRING,
	`WhereClause`  STRING,
	`CorrectionAction`  STRING NOT NULL,
	`RowIdColumns`  STRING,
	`ErrorDetails`  STRING NOT NULL,
	`SchemaName`  STRING NOT NULL,
	`ReferenceSchema`  STRING,
 CONSTRAINT `PK_ETL_Screening` PRIMARY KEY CLUSTERED 
(
	`ScreeningId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
