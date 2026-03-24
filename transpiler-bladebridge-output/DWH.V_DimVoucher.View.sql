/****** Object:  View [DWH].[V_DimVoucher]    Script Date: 03/03/2026 16:26:09 ******/






CREATE OR REPLACE VIEW `DWH`.`V_DimVoucher` AS


SELECT    UPPER(CompanyCode) AS CompanyCode
		, UPPER(Voucher) AS Voucher
		, '_N/A' AS Information

FROM DataStore3.Voucher
;
