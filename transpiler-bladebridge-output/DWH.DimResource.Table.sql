/****** Object:  Table [DWH].[DimResource]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`DimResource`(
	`DimResourceId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`ResourceCode`  STRING NOT NULL,
	`ResourceName`  STRING NOT NULL,
	`ResourceCodeName`  STRING NOT NULL,
	`ResourceGroupCode`  STRING NOT NULL,
	`ResourceGroupName`  STRING NOT NULL,
	`ResourceGroupCodeName`  STRING NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`ResourceType`  STRING NOT NULL,
	`InputWarehouseCode`  STRING NOT NULL,
	`InputWarehouseLocationCode`  STRING NOT NULL,
	`OutputWarehouseCode`  STRING NOT NULL,
	`OutputWarehouseLocationCode`  STRING NOT NULL,
	`EfficiencyPercentage`  DECIMAL(32,17) NOT NULL,
	`RouteGroupCode`  STRING NOT NULL,
	`HasFiniteSchedulingCapacity` int NOT NULL,
	`ValidFromDate` TIMESTAMP NOT NULL,
	`ValidToDate` TIMESTAMP NOT NULL,
	`CalendarCode`  STRING NOT NULL,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_DimResource` PRIMARY KEY CLUSTERED 
(
	`DimResourceId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
