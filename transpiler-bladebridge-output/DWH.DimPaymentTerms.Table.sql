/****** Object:  Table [DWH].[DimPaymentTerms]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`DimPaymentTerms`(
	`DimPaymentTermsId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`PaymentTermsCode`  STRING NOT NULL,
	`PaymentTermsName`  STRING NOT NULL,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_DimPaymentTerms` PRIMARY KEY CLUSTERED 
(
	`DimPaymentTermsId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
