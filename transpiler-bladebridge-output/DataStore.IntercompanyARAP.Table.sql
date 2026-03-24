/****** Object:  Table [DataStore].[IntercompanyARAP]    Script Date: 03/03/2026 16:26:09 ******/



CREATE OR REPLACE TABLE `DataStore`.`IntercompanyARAP`(
	`InvoiceDate` TIMESTAMP ,
	`YearInvoice` INT,
	`PostedDate` TIMESTAMP ,
	`DueDate` TIMESTAMP ,
	`Brn`  STRING NOT NULL,
	`Departement`  STRING NOT NULL,
	`AR_AP_Type`  STRING NOT NULL,
	`Type`  STRING NOT NULL,
	`SalesInvoiceCode`  STRING NOT NULL,
	`PurchaseInvoiceCode`  STRING NOT NULL,
	`JobInvoice`  STRING NOT NULL,
	`Currency`  STRING NOT NULL,
	`InvoiceTotal`  DECIMAL(38,8) NOT NULL,
	`CustomerCode`  STRING NOT NULL,
	`SupplierCode`  STRING NOT NULL,
	`AccountName`  STRING NOT NULL,
	`AP_Settlement`  STRING NOT NULL,
	`AR_Settlement`  STRING NOT NULL,
	`CrGRP`  STRING NOT NULL,
	`DrGRP`  STRING NOT NULL,
	`DestDisch`  STRING NOT NULL,
	`ETA` TIMESTAMP ,
	`ETD` TIMESTAMP ,
	`House`  STRING NOT NULL,
	`JobNumber`  STRING NOT NULL,
	`Master`  STRING NOT NULL,
	`OrigCountry`  STRING NOT NULL,
	`OrigCountryName`  STRING NOT NULL,
	`OriginLoad`  STRING NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`ExchangeRate`  DECIMAL(32,16) NOT NULL
) TEXTIMAGE_ON `PRIMARY`
;
