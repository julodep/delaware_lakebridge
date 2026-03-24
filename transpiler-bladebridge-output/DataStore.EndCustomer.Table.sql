/****** Object:  Table [DataStore].[EndCustomer]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore`.`EndCustomer`(
	`EndCustomerId` bigint NOT NULL,
	`EndCustomerCode`  STRING NOT NULL,
	`EndCustomerName`  STRING NOT NULL,
	`EndCustomerCodeName`  STRING NOT NULL,
	`DimensionName`  STRING NOT NULL
)
;
