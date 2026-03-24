/****** Object:  StoredProcedure [dbo].[sp_ssis_addlogentry]    Script Date: 03/03/2026 16:26:09 ******/




CREATE OR REPLACE PROCEDURE `dbo`.`sp_ssis_addlogentry`(
IN V_event string,
IN V_computer STRING,
IN V_operator STRING,
IN V_source STRING,
IN V_sourceid STRING,
IN V_executionid STRING,
IN V_starttime TIMESTAMP,
IN V_endtime TIMESTAMP,
IN V_datacode int,
IN V_databytes BINARY,
IN V_message message)
LANGUAGE SQL
SQL SECURITY INVOKER
AS

BEGIN
  
VALUES (      V_event,      V_computer,      V_operator,      V_source,      V_sourceid,      V_executionid,      V_starttime,      V_endtime,      V_datacode,      V_databytes,      V_message )  ;
RETURN 0
;

END
