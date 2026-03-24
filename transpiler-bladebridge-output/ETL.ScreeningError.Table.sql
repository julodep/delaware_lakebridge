/****** Object:  Table [ETL].[ScreeningError]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `ETL`.`ScreeningError`(
	`ErrorCode`  STRING,
	`RowIdColumns`  STRING,
	`RowIdValues`  STRING,
	`ErrorColumns`  STRING,
	`ErrorValues`  STRING,
	`Occurence` INT,
	`CompanyCode`  STRING,
	`ScreeningExecutionId` INT
) TEXTIMAGE_ON `PRIMARY`
;
