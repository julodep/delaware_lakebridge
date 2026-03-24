/****** Object:  Table [ETL].[RunDetail]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `ETL`.`RunDetail`(
	`ETLRunDetailsId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`TaskName`  STRING,
	`StartTime` TIMESTAMP ,
	`EndTime` TIMESTAMP ,
	`Duration`  AS (`EndTime`-`StartTime`),
	`NrOfIn` INT,
	`NrOfOut` INT,
	`NrOfNew` INT,
	`NrOfUpdates` INT,
	`NrOfDeletions` INT,
	`NrOfErrors` INT,
	`ETLRunId` INT,
	`Status`  STRING,
 CONSTRAINT `PK_ETLRunDetails` PRIMARY KEY CLUSTERED 
(
	`ETLRunDetailsId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
