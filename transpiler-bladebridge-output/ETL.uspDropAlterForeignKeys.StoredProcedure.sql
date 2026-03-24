/****** Object:  StoredProcedure [ETL].[uspDropAlterForeignKeys]    Script Date: 03/03/2026 16:26:10 ******/



CREATE OR REPLACE PROCEDURE `ETL`.`uspDropAlterForeignKeys`(
IN V_operation STRING,
IN V_tableName string,
IN V_schemaName string)
LANGUAGE SQL
SQL SECURITY INVOKER
AS

BEGIN

DECLARE VARIABLE V_cmd STRING 
;
DECLARE VARIABLE V_FK_NAME string;

DECLARE VARIABLE V_FK_OBJECTID INT;

DECLARE VARIABLE V_FK_DISABLED INT;

DECLARE VARIABLE V_FK_NOT_FOR_REPLICATION INT;

DECLARE VARIABLE V_DELETE_RULE    smallint;

DECLARE VARIABLE V_UPDATE_RULE    smallint;

DECLARE VARIABLE V_FKTABLE_NAME string;

DECLARE VARIABLE V_FKTABLE_OWNER string;

DECLARE VARIABLE V_PKTABLE_NAME string;

DECLARE VARIABLE V_PKTABLE_OWNER string;

DECLARE VARIABLE V_FKCOLUMN_NAME string;

DECLARE VARIABLE V_PKCOLUMN_NAME string;

DECLARE VARIABLE V_CONSTRAINT_COLID INT  
;
DECLARE cursor_fkeys CURSOR FOR   ;

   

   
DECLARE VARIABLE V_FKCOLUMNS STRING;

DECLARE VARIABLE V_PKCOLUMNS STRING;

DECLARE VARIABLE V_COUNTER INT  
-- create cursor to get FK columns  
;
DECLARE cursor_fkeyCols CURSOR FOR   ;

       

       
SET V_FK_NAME = (
SELECT
Fk.name
FROM    sys.foreign_keys Fk LEFT OUTER JOIN   
           sys.tables TbR ON TbR.OBJECT_ID = Fk.referenced_object_id --inner join   
   WHERE   TbR.name = V_tableName  
           AND schema_name(TbR.schema_id) = V_schemaName  

OPEN cursor_fkeys  

FETCH NEXT FROM   cursor_fkeys   
   );

   SET V_FK_OBJECTID = (
SELECT
  
           Fk.OBJECT_ID
FROM    sys.foreign_keys Fk LEFT OUTER JOIN   
           sys.tables TbR ON TbR.OBJECT_ID = Fk.referenced_object_id --inner join   
   WHERE   TbR.name = V_tableName  
           AND schema_name(TbR.schema_id) = V_schemaName  

OPEN cursor_fkeys  

FETCH NEXT FROM   cursor_fkeys   
   );

   SET   
       V_FK_DISABLED = (
SELECT
   
           Fk.is_disabled
FROM    sys.foreign_keys Fk LEFT OUTER JOIN   
           sys.tables TbR ON TbR.OBJECT_ID = Fk.referenced_object_id --inner join   
   WHERE   TbR.name = V_tableName  
           AND schema_name(TbR.schema_id) = V_schemaName  

OPEN cursor_fkeys  

FETCH NEXT FROM   cursor_fkeys   
   );

   SET   
       V_FK_NOT_FOR_REPLICATION = (
SELECT
   
           Fk.is_not_for_replication
FROM    sys.foreign_keys Fk LEFT OUTER JOIN   
           sys.tables TbR ON TbR.OBJECT_ID = Fk.referenced_object_id --inner join   
   WHERE   TbR.name = V_tableName  
           AND schema_name(TbR.schema_id) = V_schemaName  

OPEN cursor_fkeys  

FETCH NEXT FROM   cursor_fkeys   
   );

   SET   
       V_DELETE_RULE = (
SELECT
   
           Fk.delete_referential_action
FROM    sys.foreign_keys Fk LEFT OUTER JOIN   
           sys.tables TbR ON TbR.OBJECT_ID = Fk.referenced_object_id --inner join   
   WHERE   TbR.name = V_tableName  
           AND schema_name(TbR.schema_id) = V_schemaName  

OPEN cursor_fkeys  

FETCH NEXT FROM   cursor_fkeys   
   );

   SET      
       V_UPDATE_RULE = (
SELECT
   
           Fk.update_referential_action
FROM    sys.foreign_keys Fk LEFT OUTER JOIN   
           sys.tables TbR ON TbR.OBJECT_ID = Fk.referenced_object_id --inner join   
   WHERE   TbR.name = V_tableName  
           AND schema_name(TbR.schema_id) = V_schemaName  

OPEN cursor_fkeys  

FETCH NEXT FROM   cursor_fkeys   
   );

   SET      
       V_FKTABLE_NAME = (
SELECT
   
           OBJECT_NAME(Fk.parent_object_id) AS Fk_table_name
FROM    sys.foreign_keys Fk LEFT OUTER JOIN   
           sys.tables TbR ON TbR.OBJECT_ID = Fk.referenced_object_id --inner join   
   WHERE   TbR.name = V_tableName  
           AND schema_name(TbR.schema_id) = V_schemaName  

OPEN cursor_fkeys  

FETCH NEXT FROM   cursor_fkeys   
   );

   SET   
       V_FKTABLE_OWNER = (
SELECT
   
           schema_name(Fk.schema_id) AS Fk_table_schema
FROM    sys.foreign_keys Fk LEFT OUTER JOIN   
           sys.tables TbR ON TbR.OBJECT_ID = Fk.referenced_object_id --inner join   
   WHERE   TbR.name = V_tableName  
           AND schema_name(TbR.schema_id) = V_schemaName  

OPEN cursor_fkeys  

FETCH NEXT FROM   cursor_fkeys   
   );

   SET   
       V_PKTABLE_NAME = (
SELECT
   
           TbR.name AS Pk_table_name
FROM    sys.foreign_keys Fk LEFT OUTER JOIN   
           sys.tables TbR ON TbR.OBJECT_ID = Fk.referenced_object_id --inner join   
   WHERE   TbR.name = V_tableName  
           AND schema_name(TbR.schema_id) = V_schemaName  

OPEN cursor_fkeys  

FETCH NEXT FROM   cursor_fkeys   
   );

   SET   
       V_PKTABLE_OWNER = (
SELECT
   
           schema_name(TbR.schema_id) Pk_table_schema
FROM    sys.foreign_keys Fk LEFT OUTER JOIN   
           sys.tables TbR ON TbR.OBJECT_ID = Fk.referenced_object_id --inner join   
   WHERE   TbR.name = V_tableName  
           AND schema_name(TbR.schema_id) = V_schemaName  

OPEN cursor_fkeys  

FETCH NEXT FROM   cursor_fkeys   
   );
WHILE V_V_FETCH_STATUS = 0   
DO
-- create statement for enabling FK  

IF V_operation = 'ENABLE'   
   THEN
SET V_cmd = 'ALTER TABLE `' || V_FKTABLE_OWNER || '`.`' || V_FKTABLE_NAME   
           || '`  CHECK CONSTRAINT `' || V_FK_NAME || '`'  

      ;
SELECT V_cmd  
   ;
END WHILE IF  

   -- create statement for disabling FK  
;
IF V_operation = 'DISABLE'  
   THEN
SET V_cmd = 'ALTER TABLE `' || V_FKTABLE_OWNER || '`.`' || V_FKTABLE_NAME   
           || '`  NOCHECK CONSTRAINT `' || V_FK_NAME || '`'  

      ;
SELECT V_cmd  
   ;
END IF  

   -- create statement for dropping FK and also for recreating FK  
;
IF V_operation = 'DROP'  
   THEN
-- drop statement  

SET V_cmd = 'ALTER TABLE `' || V_FKTABLE_OWNER || '`.`' || V_FKTABLE_NAME   
       || '`  )
SELECT V_cmd  

       -- create process  
;
SET V_FKCOLUMN_NAME = (
SELECT
COL_NAME(Fk.parent_object_id
FROM    sys.foreign_keys Fk LEFT OUTER JOIN   
               sys.tables TbR ON TbR.OBJECT_ID = Fk.referenced_object_id INNER JOIN   
               sys.foreign_key_columns Fk_Cl ON Fk_Cl.constraint_object_id = Fk.OBJECT_ID   
       WHERE   TbR.name = V_tableName  
               AND schema_name(TbR.schema_id) = V_schemaName  
               AND Fk_Cl.constraint_object_id = V_FK_OBJECTID -- added 6/12/2008  
       ORDER BY Fk_Cl.constraint_column_id  

       OPEN cursor_fkeyCols  

       FETCH NEXT FROM    cursor_fkeyCols );

       SET V_PKCOLUMN_NAME = (
SELECT
 Fk_Cl.parent_column_id) AS Fk_col_name
FROM    sys.foreign_keys Fk LEFT OUTER JOIN   
               sys.tables TbR ON TbR.OBJECT_ID = Fk.referenced_object_id INNER JOIN   
               sys.foreign_key_columns Fk_Cl ON Fk_Cl.constraint_object_id = Fk.OBJECT_ID   
       WHERE   TbR.name = V_tableName  
               AND schema_name(TbR.schema_id) = V_schemaName  
               AND Fk_Cl.constraint_object_id = V_FK_OBJECTID -- added 6/12/2008  
       ORDER BY Fk_Cl.constraint_column_id  

       OPEN cursor_fkeyCols  

       FETCH NEXT FROM    cursor_fkeyCols );

       SET  = (
SELECT
   
               COL_NAME(Fk.referenced_object_id
FROM    sys.foreign_keys Fk LEFT OUTER JOIN   
               sys.tables TbR ON TbR.OBJECT_ID = Fk.referenced_object_id INNER JOIN   
               sys.foreign_key_columns Fk_Cl ON Fk_Cl.constraint_object_id = Fk.OBJECT_ID   
       WHERE   TbR.name = V_tableName  
               AND schema_name(TbR.schema_id) = V_schemaName  
               AND Fk_Cl.constraint_object_id = V_FK_OBJECTID -- added 6/12/2008  
       ORDER BY Fk_Cl.constraint_column_id  

       OPEN cursor_fkeyCols  

       FETCH NEXT FROM    cursor_fkeyCols );

       SET  = (
SELECT
 Fk_Cl.referenced_column_id) AS Pk_col_name
FROM    sys.foreign_keys Fk LEFT OUTER JOIN   
               sys.tables TbR ON TbR.OBJECT_ID = Fk.referenced_object_id INNER JOIN   
               sys.foreign_key_columns Fk_Cl ON Fk_Cl.constraint_object_id = Fk.OBJECT_ID   
       WHERE   TbR.name = V_tableName  
               AND schema_name(TbR.schema_id) = V_schemaName  
               AND Fk_Cl.constraint_object_id = V_FK_OBJECTID -- added 6/12/2008  
       ORDER BY Fk_Cl.constraint_column_id  

       OPEN cursor_fkeyCols  

       FETCH NEXT FROM    cursor_fkeyCols );
SET V_COUNTER = 1  
       ;
SET V_FKCOLUMNS = ''  
       ;
SET V_PKCOLUMNS = ''  
         
       ;
WHILE V_V_FETCH_STATUS = 0   
       THEN
IF V_COUNTER > 1   
           THEN
SET V_FKCOLUMNS = V_FKCOLUMNS || ','  
               ;
SET V_PKCOLUMNS = V_PKCOLUMNS || ','  
           ;
END IF  

           ;
SET V_FKCOLUMNS = V_FKCOLUMNS || '`' || V_FKCOLUMN_NAME || '`'  
           ;
SET V_PKCOLUMNS = V_PKCOLUMNS || '`' || V_PKCOLUMN_NAME || '`'  

           ;
SET V_COUNTER = V_COUNTER + 1  
             
           FETCH NEXT FROM    cursor_fkeyCols INTO V_FKCOLUMN_NAME,V_PKCOLUMN_NAME  
       ;
END IF  

       CLOSE cursor_fkeyCols   
       DEALLOCATE cursor_fkeyCols   

       -- generate create FK statement  
;
SET V_cmd = 'ALTER TABLE `' || V_FKTABLE_OWNER || '`.`' || V_FKTABLE_NAME || '`  WITH ' ||   
           CASE V_FK_DISABLED   
               WHEN 0 THEN ' CHECK '  
               WHEN 1 THEN ' NOCHECK '  
           END ||  ' ADD CONSTRAINT `' || V_FK_NAME   
           || '` FOREIGN KEY (' || V_FKCOLUMNS   
           || ') REFERENCES `' || V_PKTABLE_OWNER || '`.`' || V_PKTABLE_NAME || '` ('   
           || V_PKCOLUMNS || ') ON UPDATE ' ||   
           CASE V_UPDATE_RULE   
               WHEN 0 THEN ' NO ACTION '  
               WHEN 1 THEN ' CASCADE '   
               WHEN 2 THEN ' SET_NULL '   
               END || ' ON DELETE ' ||   
           CASE V_DELETE_RULE  
               WHEN 0 THEN ' NO ACTION '   
               WHEN 1 THEN ' CASCADE '   
               WHEN 2 THEN ' SET_NULL '   
               END || '' ||  
           CASE V_FK_NOT_FOR_REPLICATION  
               WHEN 0 THEN ''  
               WHEN 1 THEN ' NOT FOR REPLICATION '  
           END  

      ;
SELECT V_cmd  

   ;
END IF  

   FETCH NEXT FROM    cursor_fkeys   
      INTO V_FK_NAME,V_FK_OBJECTID,  
           V_FK_DISABLED,  
           V_FK_NOT_FOR_REPLICATION,  
           V_DELETE_RULE,     
           V_UPDATE_RULE,     
           V_FKTABLE_NAME,  
           V_FKTABLE_OWNER,  
           V_PKTABLE_NAME,  
           V_PKTABLE_OWNER  
;
END  

CLOSE cursor_fkeys   
DEALLOCATE cursor_fkeys

;
call sys.sp_addextendedproperty( V_name='SCDType', V_value='BK' , V_level0type='SCHEMA',V_level0name='DWH', V_level1type='TABLE',V_level1name='DimDate', V_level2type='COLUMN',V_level2name='DimDateId'
);
END
