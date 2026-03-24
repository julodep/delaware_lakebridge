/****** Object:  Table [DataStore].[ProductionCapacity]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore`.`ProductionCapacity`(
	`ProductionCapacityIdScreening`  STRING NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`PlanVersion`  STRING NOT NULL,
	`CapacityDate` TIMESTAMP NOT NULL,
	`CalendarCode`  STRING NOT NULL,
	`ResourceCode`  STRING NOT NULL,
	`RefType`  STRING NOT NULL,
	`RefCode`  STRING NOT NULL,
	`MaximumCapacity`  DECIMAL(38,6) ,
	`ReservedCapacity`  DECIMAL(38,11) ,
	`AvailableCapacity`  DECIMAL(38,17) 
)
;
