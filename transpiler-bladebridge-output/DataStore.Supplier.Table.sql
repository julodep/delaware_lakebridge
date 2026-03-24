/****** Object:  Table [DataStore].[Supplier]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore`.`Supplier`(
	`SupplierId` bigint NOT NULL,
	`CompanyCode`  STRING,
	`SupplierCode`  STRING,
	`SupplierName`  STRING NOT NULL,
	`SupplierCodeName`  STRING,
	`SupplierGroupCode`  STRING NOT NULL,
	`SupplierGroupName`  STRING NOT NULL,
	`SupplierGroupCodeName`  STRING NOT NULL,
	`Address`  STRING,
	`PostalCode`  STRING NOT NULL,
	`City`  STRING NOT NULL,
	`CountryRegionCode`  STRING NOT NULL,
	`CompanyChainName`  STRING NOT NULL
)
;
