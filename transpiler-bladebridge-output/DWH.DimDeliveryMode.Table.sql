/****** Object:  Table [DWH].[DimDeliveryMode]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`DimDeliveryMode`(
	`DimDeliveryModeId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`DeliveryModeCode`  STRING NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`DeliveryModeName`  STRING NOT NULL,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_DimDeliveryMode` PRIMARY KEY CLUSTERED 
(
	`DimDeliveryModeId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
