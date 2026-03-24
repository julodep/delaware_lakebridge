DECLARE VARIABLE V_x XML ;
/****** Object:  UserDefinedFunction [ETL].[F_SplitString]    Script Date: 03/03/2026 16:26:08 ******/



CREATE FUNCTION `ETL`.`F_SplitString` (V_Value STRING, V_Separator CHAR(1))
RETURNS V_Result TABLE(Value STRING)
AS


      

SET V_x = (
SELECT
CAST('<A>'|| REPLACE(V_Value,V_Separator,'</A><A>')|| '</A>' AS XML)  LIMIT 1);
      INSERT INTO V_Result            
      SELECT t.value('.', 'STRING') AS inVal
      FROM V_x.nodes('/A') AS x(t)
    ;
SIGNAL SQLSTATE '45000';
