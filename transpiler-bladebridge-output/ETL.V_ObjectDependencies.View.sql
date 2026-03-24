/****** Object:  View [ETL].[V_ObjectDependencies]    Script Date: 03/03/2026 16:26:09 ******/




CREATE OR REPLACE VIEW `ETL`.`V_ObjectDependencies` AS 


SELECT
    D.referencing_id AS ReferencingId,
    CAST(S.name AS STRING) AS ReferencingSchema,
    CAST(O.name AS STRING) AS ReferencingName,
    O.type AS ReferencingObjectType,
    D.referenced_id AS ReferencedId,
    D.referenced_schema_name AS ReferencedSchema,
    D.referenced_entity_name AS ReferencedName,
    RO.type AS ReferencedObjectType
FROM sys.sql_expression_dependencies D
INNER JOIN sys.objects O
    ON O.object_id=D.referencing_id
INNER JOIN sys.schemas S
    ON S.schema_id=O.schema_id
INNER JOIN sys.objects RO
    ON D.referenced_id=RO.object_id;
