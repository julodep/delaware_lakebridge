/****** Object:  Table [DataStore].[Product]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore`.`Product`(
	`ProductId` bigint NOT NULL,
	`CompanyCode`  STRING,
	`ProductCode`  STRING NOT NULL,
	`ProductName`  STRING NOT NULL,
	`ProductGroupCode`  STRING NOT NULL,
	`ProductGroupName`  STRING NOT NULL,
	`ProductGroupCodeName`  STRING NOT NULL,
	`ProductInventoryUnit`  STRING NOT NULL,
	`ProductPurchaseUnit`  STRING NOT NULL,
	`ProductSalesUnit`  STRING NOT NULL,
	`PhysicalUnitSymbol`  STRING NOT NULL,
	`PhysicalVolume`  DECIMAL(38,6) ,
	`PhysicalWeight`  DECIMAL(32,12) NOT NULL,
	`PrimaryVendorCode`  STRING,
	`CountryOfOrigin`  STRING NOT NULL,
	`IntrastatCommodityCode`  STRING NOT NULL,
	`ABCClassification`  STRING,
	`Brand`  STRING NOT NULL,
	`Material`  STRING NOT NULL,
	`BusinessType`  STRING NOT NULL
)
;
