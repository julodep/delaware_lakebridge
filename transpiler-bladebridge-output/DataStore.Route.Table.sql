/****** Object:  Table [DataStore].[Route]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore`.`Route`(
	`RouteCode`  STRING NOT NULL,
	`RouteName`  STRING NOT NULL,
	`RouteCodeName`  STRING,
	`OperationCode`  STRING NOT NULL,
	`OperationSequence` BIGINT,
	`OperationNumber` int NOT NULL,
	`OperationNumberNext` int NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`RouteGroupCode`  STRING NOT NULL,
	`RouteGroupName`  STRING NOT NULL,
	`RouteGroupCodeName`  STRING,
	`SiteCode`  STRING NOT NULL,
	`SiteName`  STRING NOT NULL
)
;
