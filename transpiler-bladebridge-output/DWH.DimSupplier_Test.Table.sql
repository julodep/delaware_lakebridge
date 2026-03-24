/****** Object:  Table [DWH].[DimSupplier_Test]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DWH`.`DimSupplier_Test`(
	`DimSupplierId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`SupplierId` bigint NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`SupplierCode`  STRING NOT NULL,
	`SupplierName`  STRING NOT NULL,
	`SupplierCodeName`  STRING NOT NULL,
	`SupplierGroupCode`  STRING NOT NULL,
	`SupplierGroupName`  STRING NOT NULL,
	`SupplierGroupCodeName`  STRING NOT NULL,
	`Address`  STRING NOT NULL,
	`PostalCode`  STRING NOT NULL,
	`City`  STRING NOT NULL,
	`CountryRegionCode`  STRING NOT NULL,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_DimSupplier1` PRIMARY KEY CLUSTERED 
(
	`DimSupplierId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
