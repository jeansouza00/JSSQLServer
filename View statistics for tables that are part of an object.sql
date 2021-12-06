USE [database_name]
GO

DECLARE @Objects NVARCHAR(MAX) = N'procedure_name,table_name,object_name,...'
    , @MinimumTableSizeMB INT = 100
    , @MinimumTableRowCount INT = 500;

WITH dependencies
AS (
    SELECT referencing_obj.object_id referencing_obj_id
        , referencing_obj.name referencing_obj_name
        , referencing_obj.type_desc referencing_obj_type
        , referenced_obj.object_id referenced_obj_id
        , referenced_obj.name referenced_obj_name
        , referenced_obj.type_desc referenced_obj_type
        , 1 AS [level]
    FROM sys.sql_expression_dependencies AS sed
    JOIN sys.objects AS referencing_obj
        ON referencing_obj.object_id = sed.referencing_id
    JOIN sys.objects AS referenced_obj
        ON referenced_obj.object_id = sed.referenced_id
            OR referenced_obj.object_id = OBJECT_ID(sed.referenced_entity_name)
    WHERE sed.referencing_id IN (
            SELECT OBJECT_ID(objects.[value]) [object_id]
            FROM STRING_SPLIT(@Objects, N',') objects
            )
        AND referencing_obj.object_id <> referenced_obj.object_id
    
    UNION ALL
    
    SELECT referencing_obj.object_id referencing_obj_id
        , referencing_obj.name referencing_obj_name
        , referencing_obj.type_desc referencing_obj_type
        , referenced_obj.object_id referenced_obj_id
        , referenced_obj.name referenced_obj_name
        , referenced_obj.type_desc referenced_obj_type
        , dependencies.[level] + 1 AS [level]
    FROM sys.sql_expression_dependencies AS sed
    JOIN sys.objects AS referencing_obj
        ON referencing_obj.object_id = sed.referencing_id
    JOIN sys.objects AS referenced_obj
        ON referenced_obj.object_id = sed.referenced_id
            OR referenced_obj.object_id = OBJECT_ID(sed.referenced_entity_name)
    JOIN dependencies
        ON dependencies.referenced_obj_id = sed.referencing_id
    WHERE referencing_obj.object_id <> referenced_obj.object_id
    )
    , tablesFilter
AS (
    SELECT t.object_id
    FROM sys.tables AS t
    INNER JOIN sys.indexes AS i
        ON t.object_id = i.object_id
    INNER JOIN sys.partitions p
        ON i.object_id = p.object_id
            AND i.index_id = p.index_id
    INNER JOIN sys.allocation_units spc
        ON p.partition_id = spc.container_id
    WHERE p.rows > @MinimumTableRowCount
    GROUP BY t.object_id
        , p.rows
    HAVING SUM(spc.total_pages) > 128 * @MinimumTableSizeMB
    )
SELECT tables.name AS table_name
    , i.type_desc AS index_type
    , s.name AS statistics_name
    , CONVERT(DATETIME, ddsp.last_updated) AS last_statistics_update
    , ddsp.rows
    , CAST(ddsp.rows_sampled AS VARCHAR) + ' (' + CAST(CAST((ddsp.rows_sampled * 100.0) / ddsp.rows AS INT) AS VARCHAR) + '%)' AS rows_sampled
    , CAST(ddsp.unfiltered_rows AS VARCHAR) + ' (' + CAST(CAST((ddsp.unfiltered_rows * 100.0) / ddsp.rows AS INT) AS VARCHAR) + '%)' AS unfiltered_rows
    , ddsp.steps AS [histogram_steps]
    , CAST(ddsp.modification_counter AS VARCHAR) + ' (' + CAST(CAST((ddsp.modification_counter * 100.0) / ddsp.rows AS INT) AS VARCHAR) + '%)' AS modification_counter
FROM (
    SELECT DISTINCT dependencies.referenced_obj_id AS [object_id]
        , dependencies.referenced_obj_name AS [name]
    FROM dependencies
    JOIN tablesFilter
        ON tablesFilter.object_id = dependencies.referenced_obj_id
    ) tables
JOIN sys.stats AS s
    ON s.object_id = tables.[object_id]
CROSS APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) AS ddsp
LEFT JOIN sys.indexes AS i
    ON i.object_id = s.object_id
        AND i.index_id = s.stats_id
ORDER BY CAST((ddsp.modification_counter * 100.0) / ddsp.rows AS INT) DESC
GO


