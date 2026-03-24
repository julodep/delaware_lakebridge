/****** Object:  View [DataStore3].[V_Voucher]    Script Date: 03/03/2026 16:26:08 ******/




CREATE OR REPLACE VIEW `DataStore3`.`V_Voucher` AS


SELECT DISTINCT CompanyCode
		, PayablesVoucher AS Voucher
FROM DataStore.AccountsPayable

UNION

SELECT DISTINCT CompanyCode
		, ReceivablesVoucher AS Voucher
FROM DataStore.AccountsReceivable

UNION

SELECT DISTINCT CompanyCode
		, Voucher AS Voucher
FROM DataStore2.GeneralLedger
;
