/****** Object:  View [DWH].[V_DimCurrency]    Script Date: 03/03/2026 16:26:08 ******/







CREATE OR REPLACE VIEW `DWH`.`V_DimCurrency` AS 


SELECT    UPPER(CurrencyCode) AS CurrencyCode
		, CurrencyName

FROM DataStore.Currency


/* Create unknown member */

UNION ALL

SELECT	'_N/A'
	  , '_N/A'
;
