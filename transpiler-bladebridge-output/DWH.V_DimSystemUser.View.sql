/****** Object:  View [DWH].[V_DimSystemUser]    Script Date: 03/03/2026 16:26:09 ******/









CREATE OR REPLACE VIEW `DWH`.`V_DimSystemUser` AS 


SELECT    UPPER(SystemUserCode) AS SystemUserCode
		, UserName
		, DomainUserName

FROM DataStore.SystemUser

/* Create unknown member */

UNION ALL

SELECT	'_N/A'
	  , '_N/A'
	  , '_N/A'
;
