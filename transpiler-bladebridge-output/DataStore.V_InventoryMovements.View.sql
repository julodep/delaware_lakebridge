/****** Object:  View [DataStore].[V_InventoryMovements]    Script Date: 03/03/2026 16:26:08 ******/









CREATE OR REPLACE VIEW `DataStore`.`V_InventoryMovements` AS


SELECT    IVS1.TransRecId
		, IVS1.DataAreaId AS CompanyCode
		, COALESCE(NULLIF(IT.CURRENCYCODE,''), '_N/A') AS Currency
		, -SUM(IT.CostAmountPhysical*IVS2.QTYSettled/IT.QTY) AS CostPhysical
		, -SUM(IT.CostAmountPosted*IVS2.QTYSettled/IT.QTY) AS CostFinancial
		, -SUM(IT.CostAmountAdjustment*IVS2.QTYSettled/IT.QTY) AS CostAdjustment

FROM dbo.SMRBIInventSettlementStaging IVS1

INNER JOIN dbo.SMRBIInventSettlementStaging IVS2
ON IVS1.SettleTransId = IVS2.SettleTransId
AND UPPER(IVS1.DataAreaId) = UPPER(IVS2.DataAreaId)
AND IVS1.InventSettlementRecId <> IVS2.InventSettlementRecId

INNER JOIN dbo.SMRBIInventTransStaging IT
ON IT.RecId = IVS2.TransRecId
AND UPPER(IT.DataAreaId) = UPPER(IVS2.DataAreaId) COLLATE DATABASE_DEFAULT

WHERE 1=1
	and IVS1.QtySettled<>0 AND IT.QTY <> 0

GROUP BY IVS1.TransRecId, IVS1.DataAreaId, IT.CURRENCYCODE
;
