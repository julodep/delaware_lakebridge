/****** Object:  Table [DataStore].[Calendar]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DataStore`.`Calendar`(
	`CalendarCode`  STRING NOT NULL,
	`CalendarName`  STRING NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`StandardWorkDayHours`  DECIMAL(32,6) NOT NULL
)
;
