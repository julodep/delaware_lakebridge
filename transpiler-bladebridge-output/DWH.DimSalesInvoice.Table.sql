/****** Object:  Table [DWH].[DimSalesInvoice]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`DimSalesInvoice`(
	`DimSalesInvoiceId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`SalesInvoiceCode`  STRING NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`CustomerCode`  STRING NOT NULL,
	`TransactionType`  STRING,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_DimSalesInvoiceId` PRIMARY KEY CLUSTERED 
(
	`DimSalesInvoiceId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
