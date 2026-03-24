/****** Object:  View [ETL].[V_ProjectParametersPaaS]    Script Date: 03/03/2026 16:26:08 ******/





CREATE OR REPLACE VIEW `ETL`.`V_ProjectParametersPaaS` AS


WITH CurrentEnvironment AS
(
	-- There can be multiple Metadata databases on the current server linked to a certain environment.
	-- But there can only be 1 Metadata database configured per environment on the current server.
	SELECT EnvironmentName
	FROM ETL.ProjectParameters
	WHERE ConfigurationFilter = 'DatabaseMetaData'
		AND ConfiguredValue = current_database()
)
SELECT 
	PP.EnvironmentName,
    PP.ConfigurationFilter,
    PP.ConfiguredValue
FROM ETL.ProjectParameters PP
INNER JOIN CurrentEnvironment CurrEnv
	ON CurrEnv.EnvironmentName = PP.EnvironmentName --
;
