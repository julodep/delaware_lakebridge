/****** Object:  Table [DWH].[DimPurchaseOrder]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`DimPurchaseOrder`(
	`DimPurchaseOrderId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`PurchaseOrderCode`  STRING NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`SupplierCode`  STRING NOT NULL,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_DimPurchaseOrderId` PRIMARY KEY CLUSTERED 
(
	`DimPurchaseOrderId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
