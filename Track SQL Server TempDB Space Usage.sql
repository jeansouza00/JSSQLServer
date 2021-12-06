/*
USE [master]
GO

CREATE DATABASE [DBAtools] ON PRIMARY (
    NAME = N'DBAtools'
    , FILENAME = N'C:\...\DBAtools.mdf'
    , SIZE = 2560 MB
    , FILEGROWTH = 256 MB
    , MAXSIZE = UNLIMITED
    ) LOG ON (
    NAME = N'DBAtools_log'
    , FILENAME = N'C:\...\DBAtools_log.ldf'
    , SIZE = 1280 MB
    , FILEGROWTH = 128 MB
    , MAXSIZE = UNLIMITED
    )
GO
*/

/*
USE [DBAtools]
GO

CREATE TABLE [TempDB_Space_Usage] (
    [run_date] [datetime] NOT NULL
    , [elapsed_time] [varchar](23) NULL
    , [session_id] [smallint] NULL
    , [database_name] [nvarchar](128) NULL
    , [query_text] [nvarchar](max) NULL
    , [query_plan] [xml] NULL
    , [status] [nvarchar](30) NULL
    , [wait_info] [nvarchar](max) NULL
    , [blocking_session_id] [smallint] NULL
    , [open_transaction_count] [int] NULL
    , [nt_domain] [nvarchar](128) NULL
    , [host_name] [nvarchar](128) NULL
    , [login_name] [nvarchar](128) NULL
    , [nt_user_name] [nvarchar](128) NULL
    , [program_name] [nvarchar](128) NULL
    , [total_alloc_user_objects] [decimal](19, 2) NULL
    , [net_alloc_user_objects] [decimal](19, 2) NULL
    , [total_alloc_internal_objects] [decimal](19, 2) NULL
    , [net_alloc_internal_objects] [decimal](19, 2) NULL
    , [total_allocation] [decimal](19, 2) NULL
    , [net_allocation] [decimal](19, 2) NULL
    , [tempdb_data_file_size] [decimal](19, 2) NULL
    , [tempdb_data_file_used_size] [decimal](19, 2) NULL
    )
GO
*/

USE [DBAtools]
GO

WITH tsu
AS (
    SELECT tsu.session_id
        , tsu.request_id
        , CAST(SUM(tsu.user_objects_alloc_page_count) / 128.0 AS DECIMAL(19, 2)) total_alloc_user_objects
        , CAST((SUM(tsu.user_objects_alloc_page_count) - SUM(tsu.user_objects_dealloc_page_count)) / 128.0 AS DECIMAL(19, 2)) net_alloc_user_objects
        , CAST(SUM(tsu.internal_objects_alloc_page_count) / 128.0 AS DECIMAL(19, 2)) total_alloc_internal_objects
        , CAST((SUM(tsu.internal_objects_alloc_page_count) - SUM(tsu.internal_objects_dealloc_page_count)) / 128.0 AS DECIMAL(19, 2)) net_alloc_internal_objects
        , CAST((SUM(tsu.user_objects_alloc_page_count) + SUM(tsu.internal_objects_alloc_page_count)) / 128.0 AS DECIMAL(19, 2)) AS total_allocation
        , CAST((SUM(tsu.user_objects_alloc_page_count) + SUM(tsu.internal_objects_alloc_page_count) - SUM(tsu.internal_objects_dealloc_page_count) - SUM(tsu.user_objects_dealloc_page_count)) / 128.0 AS DECIMAL(19, 2)) AS net_allocation
    FROM sys.dm_db_task_space_usage tsu
    WHERE tsu.user_objects_alloc_page_count > 128.0
        OR tsu.internal_objects_alloc_page_count > 128.0
    GROUP BY tsu.session_id
        , tsu.request_id
    )
    , ssu
AS (
    SELECT ssu.session_id
        , CAST(ssu.user_objects_alloc_page_count / 128.0 AS DECIMAL(19, 2)) total_alloc_user_objects
        , CAST((ssu.user_objects_alloc_page_count - ssu.user_objects_dealloc_page_count) / 128.0 AS DECIMAL(19, 2)) net_alloc_user_objects
        , CAST(ssu.internal_objects_alloc_page_count / 128.0 AS DECIMAL(19, 2)) total_alloc_internal_objects
        , CAST((ssu.internal_objects_alloc_page_count - ssu.internal_objects_dealloc_page_count) / 128.0 AS DECIMAL(19, 2)) net_alloc_internal_objects
        , CAST((ssu.user_objects_alloc_page_count + internal_objects_alloc_page_count) / 128.0 AS DECIMAL(19, 2)) AS total_allocation
        , CAST((ssu.user_objects_alloc_page_count + ssu.internal_objects_alloc_page_count - ssu.internal_objects_dealloc_page_count - ssu.user_objects_dealloc_page_count) / 128.0 AS DECIMAL(19, 2)) AS net_allocation
    FROM sys.dm_db_session_space_usage ssu
    WHERE ssu.user_objects_alloc_page_count > 128.0
        OR ssu.internal_objects_alloc_page_count > 128.0
    )
    , fsu
AS (
    SELECT CAST(SUM(fsu.total_page_count) / 128.0 AS DECIMAL(19, 2)) AS tempdb_data_file_size
        , CAST(SUM(fsu.allocated_extent_page_count) / 128.0 AS DECIMAL(19, 2)) AS tempdb_data_file_used_size
    FROM tempdb.sys.dm_db_file_space_usage AS fsu
    )
INSERT INTO [TempDB_Space_Usage]
SELECT GETDATE() AS run_date
    , RIGHT('00' + CONVERT(VARCHAR(20), (ABS(r.total_elapsed_time) / 1000) / 86400), 2) + ':' + CONVERT(VARCHAR(20), (DATEADD(SECOND, (r.total_elapsed_time / 1000), 0) + DATEADD(MILLISECOND, (r.total_elapsed_time % 1000), 0)), 114) AS [elapsed_time]
    , s.session_id
    , DB_NAME(s.database_id) AS [database_name]
    , ISNULL(SUBSTRING(t.[text], (r.statement_start_offset / 2) + 1, (
                (
                    CASE r.statement_end_offset
                        WHEN - 1
                            THEN DATALENGTH(t.[text])
                        ELSE r.statement_end_offset
                        END - r.statement_start_offset
                    ) / 2
                ) + 1), t.[text]) AS query_text
    , p.[query_plan]
    , s.[status]
    , '(' + CAST(r.wait_time AS VARCHAR) + 'ms)' + r.wait_type AS [wait_info]
    , r.blocking_session_id
    , r.open_transaction_count
    , s.nt_domain
    , s.host_name
    , s.login_name
    , s.nt_user_name
    , s.program_name
    , ISNULL(tsu.total_alloc_user_objects, 0.0) + ISNULL(ssu.total_alloc_user_objects, 0.0) AS total_alloc_user_objects
    , ISNULL(tsu.net_alloc_user_objects, 0.0) + ISNULL(ssu.net_alloc_user_objects, 0.0) AS net_alloc_user_objects
    , ISNULL(tsu.total_alloc_internal_objects, 0.0) + ISNULL(ssu.total_alloc_internal_objects, 0.0) AS total_alloc_internal_objects
    , ISNULL(tsu.net_alloc_internal_objects, 0.0) + ISNULL(ssu.net_alloc_internal_objects, 0.0) AS net_alloc_internal_objects
    , ISNULL(tsu.total_allocation, 0.0) + ISNULL(ssu.total_allocation, 0.0) AS total_allocation
    , ISNULL(tsu.net_allocation, 0.0) + ISNULL(ssu.net_allocation, 0.0) AS net_allocation
    , fsu.tempdb_data_file_size
    , fsu.tempdb_data_file_used_size
FROM tsu
LEFT JOIN ssu
    ON ssu.session_id = tsu.session_id
LEFT JOIN sys.dm_exec_sessions AS s
    ON s.session_id = tsu.session_id
LEFT JOIN sys.dm_exec_requests r
    ON r.session_id = tsu.session_id
        AND r.request_id = tsu.request_id
OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) AS t
OUTER APPLY sys.dm_exec_query_plan(r.plan_handle) AS p
CROSS JOIN fsu
GO
