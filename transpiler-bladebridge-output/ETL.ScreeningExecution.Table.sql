/****** Object:  Table [ETL].[ScreeningExecution]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `ETL`.`ScreeningExecution`(
	`ScreeningExecutionId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`ScreeningId` int NOT NULL,
	`ETLRunId` int NOT NULL,
	`StartTime` TIMESTAMP NOT NULL,
	`EndTime` TIMESTAMP ,
	`Status`  STRING NOT NULL,
	`NrOfErrors` int NOT NULL,
	`NrOfRows` int NOT NULL
)
;
