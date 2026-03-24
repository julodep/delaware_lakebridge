/****** Object:  Table [DataStore].[PriceAgreement]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DataStore`.`PriceAgreement`(
	`VendorCode`  STRING NOT NULL,
	`ProductCode`  STRING NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`Amount`  DECIMAL(32,6) NOT NULL,
	`Currency`  STRING NOT NULL,
	`FromDate` TIMESTAMP NOT NULL,
	`ToDate` TIMESTAMP NOT NULL,
	`QtyFrom`  DECIMAL(32,6) NOT NULL,
	`QtyTo`  DECIMAL(32,6) NOT NULL,
	`UnitId`  STRING NOT NULL,
	`PriceUnit`  DECIMAL(32,12) NOT NULL
)
;
