/****** Object:  Table [DataStore].[ProductionTimeRegistration]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore`.`ProductionTimeRegistration`(
	`ProductionOrderCode`  STRING NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`ProductConfigurationCode`  STRING NOT NULL,
	`RouteCode`  STRING NOT NULL,
	`RoutingName`  STRING NOT NULL,
	`ResourceCode`  STRING NOT NULL,
	`OperationCode`  STRING NOT NULL,
	`OperationNumber` int NOT NULL,
	`Shift`  STRING NOT NULL,
	`OperatorType`  STRING NOT NULL,
	`OperatorName` bigint NOT NULL,
	`RecId` bigint NOT NULL,
	`PostedJournalDate` TIMESTAMP ,
	`Hours`  DECIMAL(32,6) NOT NULL,
	`HourPrice`  DECIMAL(32,6) NOT NULL
)
;
