/****** Object:  Table [DWH].[FactProductionCapacity]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`FactProductionCapacity`(
	`FactProductionCapacityid` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`DimCompanyId` int NOT NULL,
	`DimCapacityDateId` int NOT NULL,
	`DimResourceId` int NOT NULL,
	`DimCalendarId` int NOT NULL,
	`MaximumCapacity`  DECIMAL(32,17) ,
	`ReservedCapacity`  DECIMAL(32,17) ,
	`AvailableCapacity`  DECIMAL(32,17) ,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_FactProductionCapacity` PRIMARY KEY CLUSTERED 
(
	`FactProductionCapacityid` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;

WITH CHECK ADD  CONSTRAINT `FK_DWH_FactProductionCapacity_DimCalendarId` FOREIGN KEY(`DimCalendarId`)
REFERENCES `DWH`.`DimCalendar` (`DimCalendarId`)
;

WITH CHECK ADD  CONSTRAINT `FK_DWH_FactProductionCapacity_DimCapacityDateId` FOREIGN KEY(`DimCapacityDateId`)
REFERENCES `DWH`.`DimDate` (`DimDateId`)
;

WITH NOCHECK ADD  CONSTRAINT `FK_DWH_FactProductionCapacity_DimCompanyId` FOREIGN KEY(`DimCompanyId`)
REFERENCES `DWH`.`DimCompany` (`DimCompanyId`)
;

WITH CHECK ADD  CONSTRAINT `FK_DWH_FactProductionCapacity_DimResourceId` FOREIGN KEY(`DimResourceId`)
REFERENCES `DWH`.`DimResource` (`DimResourceId`)
;
