/****** Object:  Table [DataStore].[AnalyticalDimensionLedgerSalesAndPurchase]    Script Date: 03/03/2026 16:26:08 ******/



CREATE OR REPLACE TABLE `DataStore`.`AnalyticalDimensionLedgerSalesAndPurchase`(
	`DefaultDimensionId` bigint NOT NULL,
	`MainAccount`  STRING NOT NULL,
	`Intercompany`  STRING NOT NULL,
	`BusinessSegment`  STRING NOT NULL,
	`EndCustomer`  STRING NOT NULL,
	`Department`  STRING NOT NULL,
	`LocalAccount`  STRING NOT NULL,
	`Location`  STRING NOT NULL,
	`Product`  STRING NOT NULL,
	`ShipmentContract`  STRING NOT NULL,
	`Vendor`  STRING NOT NULL
)
;
