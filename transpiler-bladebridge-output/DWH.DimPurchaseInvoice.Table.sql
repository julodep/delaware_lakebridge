/****** Object:  Table [DWH].[DimPurchaseInvoice]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`DimPurchaseInvoice`(
	`DimPurchaseInvoiceId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`PurchaseInvoiceCode`  STRING NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`SupplierCode`  STRING,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_DimPurchaseInvoiceId` PRIMARY KEY CLUSTERED 
(
	`DimPurchaseInvoiceId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
