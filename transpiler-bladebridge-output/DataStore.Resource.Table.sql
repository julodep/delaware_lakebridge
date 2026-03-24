/****** Object:  Table [DataStore].[Resource]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore`.`Resource`(
	`ResourceCode`  STRING NOT NULL,
	`ResourceName`  STRING NOT NULL,
	`ResourceCodeName`  STRING,
	`ResourceGroupCode`  STRING NOT NULL,
	`ResourceGroupName`  STRING NOT NULL,
	`ResourceGroupCodeName`  STRING,
	`CompanyCode`  STRING NOT NULL,
	`ResourceType`  STRING NOT NULL,
	`InputWarehouseCode`  STRING NOT NULL,
	`InputWarehouseLocationCode`  STRING NOT NULL,
	`OutputWarehouseCode`  STRING NOT NULL,
	`OutputWarehouseLocationCode`  STRING NOT NULL,
	`EfficiencyPercentage`  DECIMAL(32,6) ,
	`RouteGroupCode`  STRING NOT NULL,
	`HasFiniteSchedulingCapacity` INT,
	`ValidFromDate` TIMESTAMP ,
	`ValidToDate` TIMESTAMP ,
	`CalendarCode`  STRING NOT NULL
)
;
