/****** Object:  Table [DWH].[DimProduct]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DWH`.`DimProduct`(
	`DimProductId` BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	`ProductId` bigint NOT NULL,
	`CompanyCode`  STRING NOT NULL,
	`ProductCode`  STRING NOT NULL,
	`ProductName`  STRING NOT NULL,
	`ProductCodeName`  AS ((`ProductCode`||'-')+`ProductName`) PERSISTED NOT NULL,
	`ProductGroupCode`  STRING NOT NULL,
	`ProductGroupName`  STRING NOT NULL,
	`ProductGroupCodeName`  STRING NOT NULL,
	`Brand`  STRING NOT NULL,
	`Material`  STRING NOT NULL,
	`BusinessType`  STRING NOT NULL,
	`ProductInventoryUnit`  STRING NOT NULL,
	`ProductPurchaseUnit`  STRING NOT NULL,
	`ProductSalesUnit`  STRING NOT NULL,
	`PhysicalUnitSymbol`  STRING NOT NULL,
	`PhysicalVolume`  DECIMAL(38,6) NOT NULL,
	`PhysicalWeight`  DECIMAL(38,6) NOT NULL,
	`PrimaryVendorCode`  STRING NOT NULL,
	`CountryOfOrigin`  STRING NOT NULL,
	`IntrastatCommodityCode`  STRING NOT NULL,
	`ABCClassification`  STRING NOT NULL,
	`CreatedETLRunId` int NOT NULL,
	`ModifiedETLRunId` int NOT NULL,
 CONSTRAINT `PK_DimProduct` PRIMARY KEY CLUSTERED 
(
	`DimProductId` ASC
)WITH(STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF)
)
;
