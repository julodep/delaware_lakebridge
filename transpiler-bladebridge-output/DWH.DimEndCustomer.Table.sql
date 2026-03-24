/****** Object:  Table [DWH].[DimEndCustomer]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`DimEndCustomer`(
	`DimEndCustomerId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`EndCustomerId` bigint NOT NULL,
	`EndCustomerCode`  STRING NOT NULL,
	`EndCustomerName`  STRING NOT NULL,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
	`SalesSubSegmentCode`  STRING NOT NULL,
	`SalesSegmentCode`  STRING NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`CompanyChain`  STRING,
	`TaxGroup`  STRING,
 CONSTRAINT `PK_DimEndCustomer` PRIMARY KEY CLUSTERED 
(
	`DimEndCustomerId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
