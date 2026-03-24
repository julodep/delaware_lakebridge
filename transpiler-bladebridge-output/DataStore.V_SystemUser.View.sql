/****** Object:  View [DataStore].[V_SystemUser]    Script Date: 03/03/2026 16:26:08 ******/













CREATE OR REPLACE VIEW `DataStore`.`V_SystemUser` AS


SELECT	COALESCE(UPPER(DPS.User_), '_N/A') AS SystemUserCode
	  , COALESCE(SUS.UserName, '_N/A') AS UserName
	  , CAST(COALESCE(SUS.NetworkDomain || '\\' || SUS.Alias, '_N/A') AS STRING) AS DomainUserName

FROM dbo.SMRBIHcmWorkerStaging HWS

INNER JOIN dbo.SMRBIDirPersonStaging DPS
ON HWS.HcmWorkerRecId = DPS.DirPersonRecId

INNER JOIN dbo.SMRBISystemUserStaging SUS
ON DPS.User_ = SUS.UserId
;
