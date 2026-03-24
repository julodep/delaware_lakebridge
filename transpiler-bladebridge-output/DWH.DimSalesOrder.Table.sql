/****** Object:  Table [DWH].[DimSalesOrder]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`DimSalesOrder`(
	`DimSalesOrderId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`SalesOrderCode`  STRING NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`DeliveryAddress`  STRING NOT NULL,
	`RequestedShippingDate` TIMESTAMP NOT NULL,
	`RequestedDeliveryDate` TIMESTAMP NOT NULL,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_DimSalesOrderId` PRIMARY KEY CLUSTERED 
(
	`DimSalesOrderId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
