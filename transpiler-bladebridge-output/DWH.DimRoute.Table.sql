/****** Object:  Table [DWH].[DimRoute]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`DimRoute`(
	`DimRouteId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`RouteCode`  STRING NOT NULL,
	`RouteName`  STRING NOT NULL,
	`RouteCodeName`  STRING NOT NULL,
	`OperationCode`  STRING NOT NULL,
	`OperationSequence` bigint NOT NULL,
	`OperationNumber` int NOT NULL,
	`OperationNumberNext` int NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`RouteGroupCode`  STRING NOT NULL,
	`RouteGroupName`  STRING NOT NULL,
	`RouteGroupCodeName`  STRING NOT NULL,
	`SiteCode`  STRING NOT NULL,
	`SiteName`  STRING NOT NULL,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_DimRoute` PRIMARY KEY CLUSTERED 
(
	`DimRouteId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
