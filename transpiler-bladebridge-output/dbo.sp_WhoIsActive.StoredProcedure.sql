/****** Object:  StoredProcedure [dbo].[sp_WhoIsActive]    Script Date: 03/03/2026 16:26:09 ******/




/*********************************************************************************************
Who Is Active? v11.32 (2018-07-03)
(C) 2007-2018, Adam Machanic

Feedback: mailto:adam@dataeducation.com
Updates: http://whoisactive.com
Blog: http://dataeducation.com

License: 
	Who is Active? is free to download and use for personal, educational, and internal 
	corporate purposes, provided that this header is preserved. Redistribution or sale 
	of Who is Active?, in whole or in part, is prohibited without the author's express 
	written consent.
*********************************************************************************************/
CREATE OR REPLACE PROCEDURE `dbo`.`sp_WhoIsActive`(
IN V_filter string DEFAULT ",
IN V_filter_type STRING DEFAULT 'session',
IN V_not_filter string DEFAULT ",
IN V_not_filter_type STRING DEFAULT 'session',
IN V_show_own_spid BOOLEAN DEFAULT 0,
IN V_show_system_spids BOOLEAN DEFAULT 0,
IN V_show_sleeping_spids TINYINT DEFAULT 1,
IN V_get_full_inner_text BOOLEAN DEFAULT 0,
IN V_get_plans TINYINT DEFAULT 0,
IN V_get_outer_command BOOLEAN DEFAULT 0,
IN V_get_transaction_info BOOLEAN DEFAULT 0,
IN V_get_task_info TINYINT DEFAULT 1,
IN V_get_locks BOOLEAN DEFAULT 0,
IN V_get_avg_time BOOLEAN DEFAULT 0,
IN V_get_additional_info BOOLEAN DEFAULT 0,
IN V_find_block_leaders BOOLEAN DEFAULT 0,
IN V_delta_interval TINYINT DEFAULT 0,
IN V_output_column_list STRING DEFAULT '`dd%``session_id``sql_text``sql_command``login_name``wait_info``tasks``tran_log%``cpu%``temp%``block%``reads%``writes%``context%``physical%``query_plan``locks``%`',
IN V_sort_order STRING DEFAULT '`start_time` ASC',
IN V_format_output TINYINT DEFAULT 1,
IN V_destination_table STRING DEFAULT ",
IN V_return_schema BOOLEAN DEFAULT 0,
out V_schema STRING DEFAULT NULL,
IN V_help BOOLEAN DEFAULT 0)
LANGUAGE SQL
SQL SECURITY INVOKER
AS

BEGIN


DECLARE VARIABLE V_header STRING;

DECLARE VARIABLE V_params STRING;

DECLARE VARIABLE V_outputs STRING;
DECLARE VARIABLE V_recursion SMALLINT;
DECLARE VARIABLE V_first_collection_ms_ticks BIGINT;
DECLARE VARIABLE V_last_collection_start TIMESTAMP;
DECLARE VARIABLE V_sys_info BOOLEAN;
DECLARE VARIABLE V_sql STRING;

DECLARE VARIABLE V_sql_n STRING;
DECLARE VARIABLE V_i INT;
DECLARE VARIABLE V_session_id SMALLINT;

DECLARE VARIABLE V_request_id INT;

DECLARE VARIABLE V_sql_handle BINARY;

DECLARE VARIABLE V_plan_handle BINARY;

DECLARE VARIABLE V_statement_start_offset INT;

DECLARE VARIABLE V_statement_end_offset INT;

DECLARE VARIABLE V_start_time TIMESTAMP;

DECLARE VARIABLE V_database_name string;
DECLARE sql_cursor
			CURSOR LOCAL FAST_FORWARD
			FOR ;

				
DECLARE buffer_cursor
			CURSOR LOCAL FAST_FORWARD
			FOR ;

				
DECLARE VARIABLE V_live_plan BOOLEAN;
DECLARE plan_cursor
			CURSOR LOCAL FAST_FORWARD
			FOR ;

				
DECLARE VARIABLE V_query_plan XML;
DECLARE locks_cursor
			CURSOR LOCAL FAST_FORWARD
			FOR ;

				
DECLARE blocks_cursor
			CURSOR LOCAL FAST_FORWARD
			FOR;

				
DECLARE VARIABLE V_job_id STRING;
DECLARE VARIABLE V_step_id INT;
DECLARE agent_cursor
			CURSOR LOCAL FAST_FORWARD
			FOR ;

				
DECLARE VARIABLE V_delay_time CHAR(12);/*
OUTPUT COLUMNS
--------------
Formatted/Non:	[session_id] [smallint] NOT NULL
	Session ID (a.k.a. SPID)

Formatted:		[dd hh:mm:ss.mss] [varchar](15) NULL
Non-Formatted:	<not returned>
	For an active request, time the query has been running
	For a sleeping session, time since the last batch completed

Formatted:		[dd hh:mm:ss.mss (avg)] [varchar](15) NULL
Non-Formatted:	[avg_elapsed_time] [int] NULL
	(Requires @get_avg_time option)
	How much time has the active portion of the query taken in the past, on average?

Formatted:		[physical_io] [varchar](30) NULL
Non-Formatted:	[physical_io] [bigint] NULL
	Shows the number of physical I/Os, for active requests

Formatted:		[reads] [varchar](30) NULL
Non-Formatted:	[reads] [bigint] NULL
	For an active request, number of reads done for the current query
	For a sleeping session, total number of reads done over the lifetime of the session

Formatted:		[physical_reads] [varchar](30) NULL
Non-Formatted:	[physical_reads] [bigint] NULL
	For an active request, number of physical reads done for the current query
	For a sleeping session, total number of physical reads done over the lifetime of the session

Formatted:		[writes] [varchar](30) NULL
Non-Formatted:	[writes] [bigint] NULL
	For an active request, number of writes done for the current query
	For a sleeping session, total number of writes done over the lifetime of the session

Formatted:		[tempdb_allocations] [varchar](30) NULL
Non-Formatted:	[tempdb_allocations] [bigint] NULL
	For an active request, number of TempDB writes done for the current query
	For a sleeping session, total number of TempDB writes done over the lifetime of the session

Formatted:		[tempdb_current] [varchar](30) NULL
Non-Formatted:	[tempdb_current] [bigint] NULL
	For an active request, number of TempDB pages currently allocated for the query
	For a sleeping session, number of TempDB pages currently allocated for the session

Formatted:		[CPU] [varchar](30) NULL
Non-Formatted:	[CPU] [int] NULL
	For an active request, total CPU time consumed by the current query
	For a sleeping session, total CPU time consumed over the lifetime of the session

Formatted:		[context_switches] [varchar](30) NULL
Non-Formatted:	[context_switches] [bigint] NULL
	Shows the number of context switches, for active requests

Formatted:		[used_memory] [varchar](30) NOT NULL
Non-Formatted:	[used_memory] [bigint] NOT NULL
	For an active request, total memory consumption for the current query
	For a sleeping session, total current memory consumption

Formatted:		[physical_io_delta] [varchar](30) NULL
Non-Formatted:	[physical_io_delta] [bigint] NULL
	(Requires @delta_interval option)
	Difference between the number of physical I/Os reported on the first and second collections. 
	If the request started after the first collection, the value will be NULL

Formatted:		[reads_delta] [varchar](30) NULL
Non-Formatted:	[reads_delta] [bigint] NULL
	(Requires @delta_interval option)
	Difference between the number of reads reported on the first and second collections. 
	If the request started after the first collection, the value will be NULL

Formatted:		[physical_reads_delta] [varchar](30) NULL
Non-Formatted:	[physical_reads_delta] [bigint] NULL
	(Requires @delta_interval option)
	Difference between the number of physical reads reported on the first and second collections. 
	If the request started after the first collection, the value will be NULL

Formatted:		[writes_delta] [varchar](30) NULL
Non-Formatted:	[writes_delta] [bigint] NULL
	(Requires @delta_interval option)
	Difference between the number of writes reported on the first and second collections. 
	If the request started after the first collection, the value will be NULL

Formatted:		[tempdb_allocations_delta] [varchar](30) NULL
Non-Formatted:	[tempdb_allocations_delta] [bigint] NULL
	(Requires @delta_interval option)
	Difference between the number of TempDB writes reported on the first and second collections. 
	If the request started after the first collection, the value will be NULL

Formatted:		[tempdb_current_delta] [varchar](30) NULL
Non-Formatted:	[tempdb_current_delta] [bigint] NULL
	(Requires @delta_interval option)
	Difference between the number of allocated TempDB pages reported on the first and second 
	collections. If the request started after the first collection, the value will be NULL

Formatted:		[CPU_delta] [varchar](30) NULL
Non-Formatted:	[CPU_delta] [int] NULL
	(Requires @delta_interval option)
	Difference between the CPU time reported on the first and second collections. 
	If the request started after the first collection, the value will be NULL

Formatted:		[context_switches_delta] [varchar](30) NULL
Non-Formatted:	[context_switches_delta] [bigint] NULL
	(Requires @delta_interval option)
	Difference between the context switches count reported on the first and second collections
	If the request started after the first collection, the value will be NULL

Formatted:		[used_memory_delta] [varchar](30) NULL
Non-Formatted:	[used_memory_delta] [bigint] NULL
	Difference between the memory usage reported on the first and second collections
	If the request started after the first collection, the value will be NULL

Formatted:		[tasks] [varchar](30) NULL
Non-Formatted:	[tasks] [smallint] NULL
	Number of worker tasks currently allocated, for active requests

Formatted/Non:	[status] [varchar](30) NOT NULL
	Activity status for the session (running, sleeping, etc)

Formatted/Non:	[wait_info] [nvarchar](4000) NULL
	Aggregates wait information, in the following format:
		(Ax: Bms/Cms/Dms)E
	A is the number of waiting tasks currently waiting on resource type E. B/C/D are wait
	times, in milliseconds. If only one thread is waiting, its wait time will be shown as B.
	If two tasks are waiting, each of their wait times will be shown (B/C). If three or more 
	tasks are waiting, the minimum, average, and maximum wait times will be shown (B/C/D).
	If wait type E is a page latch wait and the page is of a "special" type (e.g. PFS, GAM, SGAM), 
	the page type will be identified.
	If wait type E is CXPACKET, the nodeId from the query plan will be identified

Formatted/Non:	[locks] [xml] NULL
	(Requires @get_locks option)
	Aggregates lock information, in XML format.
	The lock XML includes the lock mode, locked object, and aggregates the number of requests. 
	Attempts are made to identify locked objects by name

Formatted/Non:	[tran_start_time] [datetime] NULL
	(Requires @get_transaction_info option)
	Date and time that the first transaction opened by a session caused a transaction log 
	write to occur.

Formatted/Non:	[tran_log_writes] [nvarchar](4000) NULL
	(Requires @get_transaction_info option)
	Aggregates transaction log write information, in the following format:
	A:wB (C kB)
	A is a database that has been touched by an active transaction
	B is the number of log writes that have been made in the database as a result of the transaction
	C is the number of log kilobytes consumed by the log records

Formatted:		[open_tran_count] [varchar](30) NULL
Non-Formatted:	[open_tran_count] [smallint] NULL
	Shows the number of open transactions the session has open

Formatted:		[sql_command] [xml] NULL
Non-Formatted:	[sql_command] [nvarchar](max) NULL
	(Requires @get_outer_command option)
	Shows the "outer" SQL command, i.e. the text of the batch or RPC sent to the server, 
	if available

Formatted:		[sql_text] [xml] NULL
Non-Formatted:	[sql_text] [nvarchar](max) NULL
	Shows the SQL text for active requests or the last statement executed
	for sleeping sessions, if available in either case.
	If @get_full_inner_text option is set, shows the full text of the batch.
	Otherwise, shows only the active statement within the batch.
	If the query text is locked, a special timeout message will be sent, in the following format:
		<timeout_exceeded />
	If an error occurs, an error message will be sent, in the following format:
		<error message="message" />

Formatted/Non:	[query_plan] [xml] NULL
	(Requires @get_plans option)
	Shows the query plan for the request, if available.
	If the plan is locked, a special timeout message will be sent, in the following format:
		<timeout_exceeded />
	If an error occurs, an error message will be sent, in the following format:
		<error message="message" />

Formatted/Non:	[blocking_session_id] [smallint] NULL
	When applicable, shows the blocking SPID

Formatted:		[blocked_session_count] [varchar](30) NULL
Non-Formatted:	[blocked_session_count] [smallint] NULL
	(Requires @find_block_leaders option)
	The total number of SPIDs blocked by this session,
	all the way down the blocking chain.

Formatted:		[percent_complete] [varchar](30) NULL
Non-Formatted:	[percent_complete] [real] NULL
	When applicable, shows the percent complete (e.g. for backups, restores, and some rollbacks)

Formatted/Non:	[host_name] [sysname] NOT NULL
	Shows the host name for the connection

Formatted/Non:	[login_name] [sysname] NOT NULL
	Shows the login name for the connection

Formatted/Non:	[database_name] [sysname] NULL
	Shows the connected database

Formatted/Non:	[program_name] [sysname] NULL
	Shows the reported program/application name

Formatted/Non:	[additional_info] [xml] NULL
	(Requires @get_additional_info option)
	Returns additional non-performance-related session/request information
	If the script finds a SQL Agent job running, the name of the job and job step will be reported
	If @get_task_info = 2 and the script finds a lock wait, the locked object will be reported

Formatted/Non:	[start_time] [datetime] NOT NULL
	For active requests, shows the time the request started
	For sleeping sessions, shows the time the last batch completed

Formatted/Non:	[login_time] [datetime] NOT NULL
	Shows the time that the session connected

Formatted/Non:	[request_id] [int] NULL
	For active requests, shows the request_id
	Should be 0 unless MARS is being used

Formatted/Non:	[collection_time] [datetime] NOT NULL
	Time that this script's final SELECT ran
*/
AS
BEGIN


	SET NUMERIC_ROUNDABORT OFF;
IF
		V_filter IS NULL
		OR V_filter_type IS NULL
		OR V_not_filter IS NULL
		OR V_not_filter_type IS NULL
		OR V_show_own_spid IS NULL
		OR V_show_system_spids IS NULL
		OR V_show_sleeping_spids IS NULL
		OR V_get_full_inner_text IS NULL
		OR V_get_plans IS NULL
		OR V_get_outer_command IS NULL
		OR V_get_transaction_info IS NULL
		OR V_get_task_info IS NULL
		OR V_get_locks IS NULL
		OR V_get_avg_time IS NULL
		OR V_get_additional_info IS NULL
		OR V_find_block_leaders IS NULL
		OR V_delta_interval IS NULL
		OR V_format_output IS NULL
		OR V_output_column_list IS NULL
		OR V_sort_order IS NULL
		OR V_return_schema IS NULL
		OR V_destination_table IS NULL
		OR V_help IS NULL
	THEN
RESIGNAL;
SIGNAL SQLSTATE '45000';
END IF;
IF V_filter_type NOT IN ('session', 'program', 'database', 'login', 'host')
	THEN
RESIGNAL;
SIGNAL SQLSTATE '45000';
END IF;
IF V_filter_type = 'session' AND V_filter LIKE '%`^0123456789`%'
	THEN
RESIGNAL;
SIGNAL SQLSTATE '45000';
END IF;
IF V_not_filter_type NOT IN ('session', 'program', 'database', 'login', 'host')
	THEN
RESIGNAL;
SIGNAL SQLSTATE '45000';
END IF;
IF V_not_filter_type = 'session' AND V_not_filter LIKE '%`^0123456789`%'
	THEN
RESIGNAL;
SIGNAL SQLSTATE '45000';
END IF;
IF V_show_sleeping_spids NOT IN (0, 1, 2)
	THEN
RESIGNAL;
SIGNAL SQLSTATE '45000';
END IF;
IF V_get_plans NOT IN (0, 1, 2)
	THEN
RESIGNAL;
SIGNAL SQLSTATE '45000';
END IF;
IF V_get_task_info NOT IN (0, 1, 2)
	THEN
RESIGNAL;
SIGNAL SQLSTATE '45000';
END IF;
IF V_format_output NOT IN (0, 1, 2)
	THEN
RESIGNAL;
SIGNAL SQLSTATE '45000';
END IF;
IF V_help = 1
	THEN

SET V_header = (
SELECT
REPLACE
(
REPLACE
(
CAST(SUBSTRING
(
t.STRING, 
INSTR(t.STRING, '/' + REPEAT('*', 93)) + 94,
INSTR(t.STRING, REPEAT('*', 93) + '/') - (INSTR(t.STRING, '/' + REPEAT('*', 93)) + 94)
) AS STRING),
CHAR(13)+CHAR(10),
CHAR(13)
),
'	',
''
) FROM sys.dm_exec_requests AS r
		CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) AS t
		WHERE
			r.session_id = V_V_SPID LIMIT 1);
SET V_params = (
SELECT
CHAR(13) +
REPLACE
(
REPLACE
(
CAST(SUBSTRING
(
t.STRING, 
INSTR(t.STRING, '--~') + 5, 
INSTR(SUBSTRING(t.STRING, INSTR(t.STRING, '--~') + 5), '--~') - (INSTR(t.STRING, '--~') + 5)
) AS STRING),
CHAR(13)+CHAR(10),
CHAR(13)
),
'	',
''
) FROM sys.dm_exec_requests AS r
		CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) AS t
		WHERE
			r.session_id = V_V_SPID LIMIT 1);
SET V_outputs = (
SELECT
CHAR(13) +
REPLACE
(
REPLACE
(
REPLACE
(
CAST(SUBSTRING
(
t.STRING, 
INSTR(t.STRING, 'OUTPUT COLUMNS'+CHAR(13)+CHAR(10)+'--------------') + 32,
INSTR(SUBSTRING(t.STRING, INSTR(t.STRING, 'OUTPUT COLUMNS'+CHAR(13)+CHAR(10)+'--------------') + 32), '*/') - (INSTR(t.STRING, 'OUTPUT COLUMNS'+CHAR(13)+CHAR(10)+'--------------') + 32)
) AS STRING),
CHAR(9),
CHAR(255)
),
CHAR(13)+CHAR(10),
CHAR(13)
),
'	',
''
) +
CHAR(13) FROM sys.dm_exec_requests AS r
		CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) AS t
		WHERE
			r.session_id = V_V_SPID LIMIT 1);
WITH
		a0 AS
		(SELECT 1 AS n UNION ALL SELECT 1),
		a1 AS
		(SELECT 1 AS n FROM a0 AS a, a0 AS b),
		a2 AS
		(SELECT 1 AS n FROM a1 AS a, a1 AS b),
		a3 AS
		(SELECT 1 AS n FROM a2 AS a, a2 AS b),
		a4 AS
		(SELECT 1 AS n FROM a3 AS a, a3 AS b),
		numbers AS
		(
			SELECT TOP(LEN(V_header) - 1)
				ROW_NUMBER() OVER
				(
					ORDER BY (SELECT NULL)
				) AS BIGINT
			FROM a4
			ORDER BY
				BIGINT
		)
		SELECT
			RTRIM(LTRIM(
				SUBSTRING
				(
					V_header,
					BIGINT + 1,
					INSTR(SUBSTRING(V_header, BIGINT + 1), CHAR(13)) - BIGINT - 1
				)
			)) AS `------header---------------------------------------------------------------------------------------------------------------]
		FROM_numbers
		WHERE
			SUBSTRING_V_header__number__1____CHAR(13)_

		;
WITH
		a0_AS
		_SELECT_1_AS_n_UNION_ALL_SELECT_1__
		a1_AS
		_SELECT_1_AS_n_FROM_a0_AS_a__a0_AS_b__
		a2_AS
		_SELECT_1_AS_n_FROM_a1_AS_a__a1_AS_b__
		a3_AS
		_SELECT_1_AS_n_FROM_a2_AS_a__a2_AS_b__
		a4_AS
		_SELECT_1_AS_n_FROM_a3_AS_a__a3_AS_b__
		numbers_AS
		_
			SELECT_TOP_LEN_V_params__-_1_
				ROW_NUMBER___OVER
				_
					ORDER_BY__SELECT_NULL_
				__AS_number
			FROM_a4
			ORDER_BY
				BIGINT
		__
		tokens_AS
		_
			SELECT_
				RTRIM_LTRIM_
					SUBSTRING
					_
						V_params_
						number_+_1_
						CHARINDEX_CHAR(13)__V_params__number_+_1__-_number_-_1
					_
				___AS_token_
				number_
				CASE
					WHEN SUBSTRING(V_params, BIGINT + 1, 1) = CHAR(13) THEN BIGINT
					ELSE COALESCE(NULLIF(INSTR(SUBSTRING(V_params, BIGINT), ',' + CHAR(13) + CHAR(13)), 0), LEN(V_params)) 
				END_AS_param_group_
				ROW_NUMBER___OVER
				_
					PARTITION_BY
						CHARINDEX_','_+_CHAR(13)_+_CHAR(13)__V_params__number__
						SUBSTRING_V_params__number+1__1_
					ORDER_BY_
						BIGINT
				__AS_group_order
			FROM_numbers
			WHERE
				SUBSTRING_V_params__number__1____CHAR(13)
		__
		parsed_tokens_AS
		_
			SELECT
				MIN
				_
					CASE
						WHEN token LIKE '@%' THEN token
						ELSE NULL
					END
				__AS_parameter_
				MIN
				_
					CASE
						WHEN token LIKE '--%' THEN RIGHT(token, LEN(token) - 2)
						ELSE NULL
					END
				__AS_description_
				param_group_
				group_order
			FROM_tokens
			WHERE
				NOT_
				_
					token___''_
					AND_group_order_>_1
				_
			GROUP_BY
				param_group_
				group_order
		_;

		SELECT
			CASE
				WHEN description IS NULL AND parameter IS NULL THEN '-------------------------------------------------------------------------'
				WHEN param_group = MAX(param_group) OVER() THEN parameter
				ELSE COALESCE(LEFT(parameter, LEN(parameter) - 1), '')
			END_AS_`------parameter----------------------------------------------------------],
			CASE
				WHEN description IS NULL AND parameter IS NULL THEN '----------------------------------------------------------------------------------------------------------------------'
				ELSE COALESCE(description, '')
			END AS `------description-----------------------------------------------------------------------------------------------------]
		FROM_parsed_tokens
		ORDER_BY
			param_group__
			group_order_
		
		;
WITH
		a0_AS
		_SELECT_1_AS_n_UNION_ALL_SELECT_1__
		a1_AS
		_SELECT_1_AS_n_FROM_a0_AS_a__a0_AS_b__
		a2_AS
		_SELECT_1_AS_n_FROM_a1_AS_a__a1_AS_b__
		a3_AS
		_SELECT_1_AS_n_FROM_a2_AS_a__a2_AS_b__
		a4_AS
		_SELECT_1_AS_n_FROM_a3_AS_a__a3_AS_b__
		numbers_AS
		_
			SELECT_TOP_LEN_V_outputs__-_1_
				ROW_NUMBER___OVER
				_
					ORDER_BY__SELECT_NULL_
				__AS_number
			FROM_a4
			ORDER_BY
				BIGINT
		__
		tokens_AS
		_
			SELECT_
				RTRIM_LTRIM_
					SUBSTRING
					_
						V_outputs_
						number_+_1_
						CASE
							WHEN 
								COALESCE(NULLIF(INSTR(SUBSTRING(V_outputs, BIGINT + 1), CHAR(13) + 'Formatted'), 0), LEN(V_outputs)) < 
								COALESCE(NULLIF(INSTR(SUBSTRING(V_outputs, BIGINT + 1), CHAR(13) + CHAR(255) COLLATE Latin1_General_Bin2), 0), LEN(V_outputs))
								THEN COALESCE(NULLIF(INSTR(SUBSTRING(V_outputs, BIGINT + 1), CHAR(13) + 'Formatted'), 0), LEN(V_outputs)) - BIGINT - 1
							ELSE
								COALESCE(NULLIF(INSTR(SUBSTRING(V_outputs, BIGINT + 1), CHAR(13) + CHAR(255) COLLATE Latin1_General_Bin2), 0), LEN(V_outputs)) - BIGINT - 1
						END
					_
				___AS_token_
				number_
				COALESCE_NULLIF_CHARINDEX_CHAR(13)_+_'Formatted'__V_outputs__number_+_1___0___LEN_V_outputs___AS_output_group_
				ROW_NUMBER___OVER
				_
					PARTITION_BY_
						COALESCE_NULLIF_CHARINDEX_CHAR(13)_+_'Formatted'__V_outputs__number_+_1___0___LEN_V_outputs__
					ORDER_BY
						BIGINT
				__AS_output_group_order
			FROM_numbers
			WHERE
				SUBSTRING_V_outputs__number__10____CHAR(13)_+_'Formatted'
				OR_SUBSTRING_V_outputs__number__2____CHAR(13)_+_CHAR(255)_COLLATE_Latin1_General_Bin2
		__
		output_tokens_AS
		_
			SELECT_
				*_
				CASE output_group_order
					WHEN 2 THEN MAX(CASE output_group_order WHEN 1 THEN token ELSE NULL END) OVER (PARTITION BY output_group)
					ELSE ''
				END_COLLATE_Latin1_General_Bin2_AS_column_info
			FROM_tokens
		_
		SELECT
			CASE output_group_order
				WHEN 1 THEN '-----------------------------------'
				WHEN 2 THEN 
					CASE
						WHEN INSTR(column_info, 'Formatted/Non:') = 1 THEN
							SUBSTRING(column_info, INSTR(column_info, CHAR(255) COLLATE Latin1_General_Bin2)+1, INSTR(SUBSTRING(column_info, INSTR(column_info, CHAR(255) COLLATE Latin1_General_Bin2)+2), ']') - INSTR(column_info, CHAR(255) COLLATE Latin1_General_Bin2))
						ELSE
							SUBSTRING(column_info, INSTR(column_info, CHAR(255) COLLATE Latin1_General_Bin2)+2, INSTR(SUBSTRING(column_info, INSTR(column_info, CHAR(255) COLLATE Latin1_General_Bin2)+2), ']') - INSTR(column_info, CHAR(255) COLLATE Latin1_General_Bin2)-1)
					END
				ELSE ''
			END_AS_formatted_column_name_
			CASE output_group_order
				WHEN 1 THEN '-----------------------------------'
				WHEN 2 THEN 
					CASE
						WHEN INSTR(column_info, 'Formatted/Non:') = 1 THEN
							SUBSTRING(column_info, INSTR(column_info, ']')+2, LEN(column_info))
						ELSE
							SUBSTRING(column_info, INSTR(column_info, ']')+2, INSTR(SUBSTRING(column_info, INSTR(column_info, ']')+2), 'Non-Formatted:') - INSTR(column_info, ']')-3)
					END
				ELSE ''
			END_AS_formatted_column_type_
			CASE output_group_order
				WHEN 1 THEN '---------------------------------------'
				WHEN 2 THEN 
					CASE
						WHEN INSTR(column_info, 'Formatted/Non:') = 1 THEN ''
						ELSE
							CASE
								WHEN SUBSTRING(column_info, INSTR(SUBSTRING(column_info, INSTR(column_info, 'Non-Formatted:')), CHAR(255) COLLATE Latin1_General_Bin2)+1, 1) = '<' THEN
									SUBSTRING(column_info, INSTR(SUBSTRING(column_info, INSTR(column_info, 'Non-Formatted:')), CHAR(255) COLLATE Latin1_General_Bin2)+1, INSTR(SUBSTRING(column_info, INSTR(SUBSTRING(column_info, INSTR(column_info, 'Non-Formatted:')), CHAR(255) COLLATE Latin1_General_Bin2)+1), '>') - INSTR(SUBSTRING(column_info, INSTR(column_info, 'Non-Formatted:')), CHAR(255) COLLATE Latin1_General_Bin2))
								ELSE
									SUBSTRING(column_info, INSTR(SUBSTRING(column_info, INSTR(column_info, 'Non-Formatted:')), CHAR(255) COLLATE Latin1_General_Bin2)+1, INSTR(SUBSTRING(column_info, INSTR(SUBSTRING(column_info, INSTR(column_info, 'Non-Formatted:')), CHAR(255) COLLATE Latin1_General_Bin2)+1), ']') - INSTR(SUBSTRING(column_info, INSTR(column_info, 'Non-Formatted:')), CHAR(255) COLLATE Latin1_General_Bin2))
							END
					END
				ELSE ''
			END_AS_unformatted_column_name_
			CASE output_group_order
				WHEN 1 THEN '---------------------------------------'
				WHEN 2 THEN 
					CASE
						WHEN INSTR(column_info, 'Formatted/Non:') = 1 THEN ''
						ELSE
							CASE
								WHEN SUBSTRING(column_info, INSTR(SUBSTRING(column_info, INSTR(column_info, 'Non-Formatted:')), CHAR(255) COLLATE Latin1_General_Bin2)+1, 1) = '<' THEN ''
								ELSE
									SUBSTRING(column_info, INSTR(SUBSTRING(column_info, INSTR(column_info, 'Non-Formatted:')), ']')+2, INSTR(SUBSTRING(column_info, INSTR(column_info, ']')+2), 'Non-Formatted:') - INSTR(column_info, ']')-3)
							END
					END
				ELSE ''
			END_AS_unformatted_column_type_
			CASE output_group_order
				WHEN 1 THEN '----------------------------------------------------------------------------------------------------------------------'
				ELSE REPLACE(token, CHAR(255) COLLATE Latin1_General_Bin2, '')
			END_AS_`------description-----------------------------------------------------------------------------------------------------]
		FROM output_tokens
		WHERE
			NOT 
			(
				output_group_order = 1 
				AND output_group = LEN(V_outputs)
			)
		ORDER BY
			output_group,
			CASE output_group_order
				WHEN 1 THEN 99
				ELSE output_group_order
			END;
SIGNAL SQLSTATE '45000';
END IF;
SET (V_output_column_list) = (
WITH
	a0 AS
	(SELECT 1 AS n UNION ALL SELECT 1),
	a1 AS
	(SELECT 1 AS n FROM a0 AS a, a0 AS b),
	a2 AS
	(SELECT 1 AS n FROM a1 AS a, a1 AS b),
	a3 AS
	(SELECT 1 AS n FROM a2 AS a, a2 AS b),
	a4 AS
	(SELECT 1 AS n FROM a3 AS a, a3 AS b),
	numbers AS
	(
		SELECT TOP(LEN(V_output_column_list))
			ROW_NUMBER() OVER
			(
				ORDER BY (SELECT NULL)
			) AS BIGINT
		FROM a4
		ORDER BY
			BIGINT
	),
	tokens AS
	(
		SELECT 
			'|`' ||
				SUBSTRING
				(
					V_output_column_list,
					BIGINT + 1,
					INSTR(SUBSTRING(V_output_column_list, BIGINT), '`') - BIGINT - 1
				) || '|`' AS token,
			BIGINT
		FROM numbers
		WHERE
			SUBSTRING(V_output_column_list, BIGINT, 1) = '`'
	),
	ordered_columns AS
	(
		SELECT
			x.column_name,
			ROW_NUMBER() OVER
			(
				PARTITION BY
					x.column_name
				ORDER BY
					tokens.number,
					x.default_order
			) AS r,
			ROW_NUMBER() OVER
			(
				ORDER BY
					tokens.number,
					x.default_order
			) AS s
		FROM tokens
		JOIN
		(
			SELECT '`session_id`' AS column_name, 1 AS default_order
			UNION ALL
			SELECT '`dd hh:mm:ss.mss`', 2
			WHERE
				V_format_output IN (1, 2)
			UNION ALL
			SELECT '`dd hh:mm:ss.mss (avg)`', 3
			WHERE
				V_format_output IN (1, 2)
				AND V_get_avg_time = 1
			UNION ALL
			SELECT '`avg_elapsed_time`', 4
			WHERE
				V_format_output = 0
				AND V_get_avg_time = 1
			UNION ALL
			SELECT '`physical_io`', 5
			WHERE
				V_get_task_info = 2
			UNION ALL
			SELECT '`reads`', 6
			UNION ALL
			SELECT '`physical_reads`', 7
			UNION ALL
			SELECT '`writes`', 8
			UNION ALL
			SELECT '`tempdb_allocations`', 9
			UNION ALL
			SELECT '`tempdb_current`', 10
			UNION ALL
			SELECT '`CPU`', 11
			UNION ALL
			SELECT '`context_switches`', 12
			WHERE
				V_get_task_info = 2
			UNION ALL
			SELECT '`used_memory`', 13
			UNION ALL
			SELECT '`physical_io_delta`', 14
			WHERE
				V_delta_interval > 0	
				AND V_get_task_info = 2
			UNION ALL
			SELECT '`reads_delta`', 15
			WHERE
				V_delta_interval > 0
			UNION ALL
			SELECT '`physical_reads_delta`', 16
			WHERE
				V_delta_interval > 0
			UNION ALL
			SELECT '`writes_delta`', 17
			WHERE
				V_delta_interval > 0
			UNION ALL
			SELECT '`tempdb_allocations_delta`', 18
			WHERE
				V_delta_interval > 0
			UNION ALL
			SELECT '`tempdb_current_delta`', 19
			WHERE
				V_delta_interval > 0
			UNION ALL
			SELECT '`CPU_delta`', 20
			WHERE
				V_delta_interval > 0
			UNION ALL
			SELECT '`context_switches_delta`', 21
			WHERE
				V_delta_interval > 0
				AND V_get_task_info = 2
			UNION ALL
			SELECT '`used_memory_delta`', 22
			WHERE
				V_delta_interval > 0
			UNION ALL
			SELECT '`tasks`', 23
			WHERE
				V_get_task_info = 2
			UNION ALL
			SELECT '`status`', 24
			UNION ALL
			SELECT '`wait_info`', 25
			WHERE
				V_get_task_info > 0
				OR V_find_block_leaders = 1
			UNION ALL
			SELECT '`locks`', 26
			WHERE
				V_get_locks = 1
			UNION ALL
			SELECT '`tran_start_time`', 27
			WHERE
				V_get_transaction_info = 1
			UNION ALL
			SELECT '`tran_log_writes`', 28
			WHERE
				V_get_transaction_info = 1
			UNION ALL
			SELECT '`open_tran_count`', 29
			UNION ALL
			SELECT '`sql_command`', 30
			WHERE
				V_get_outer_command = 1
			UNION ALL
			SELECT '`sql_text`', 31
			UNION ALL
			SELECT '`query_plan`', 32
			WHERE
				V_get_plans >= 1
			UNION ALL
			SELECT '`blocking_session_id`', 33
			WHERE
				V_get_task_info > 0
				OR V_find_block_leaders = 1
			UNION ALL
			SELECT '`blocked_session_count`', 34
			WHERE
				V_find_block_leaders = 1
			UNION ALL
			SELECT '`percent_complete`', 35
			UNION ALL
			SELECT '`host_name`', 36
			UNION ALL
			SELECT '`login_name`', 37
			UNION ALL
			SELECT '`database_name`', 38
			UNION ALL
			SELECT '`program_name`', 39
			UNION ALL
			SELECT '`additional_info`', 40
			WHERE
				V_get_additional_info = 1
			UNION ALL
			SELECT '`start_time`', 41
			UNION ALL
			SELECT '`login_time`', 42
			UNION ALL
			SELECT '`request_id`', 43
			UNION ALL
			SELECT '`collection_time`', 44
		) AS x ON 
			x.column_name LIKE token ESCAPE '|'
	)
SELECT
		CONCAT(substring((
SELECT
',' || column_name as `text__`
FROM ordered_columns
WHERE
r = 1
ORDER BY
s
FOR XML
PATH('')
), 1, 1 - 1), '', substring((
SELECT
',' || column_name as `text__`
FROM ordered_columns
WHERE
r = 1
ORDER BY
s
FOR XML
PATH('')
), 1 + 1));
);
IF COALESCE(RTRIM(V_output_column_list), '') = ''
	THEN
RESIGNAL;
SIGNAL SQLSTATE '45000';
END IF;
IF V_destination_table <> ''
	THEN
SET V_destination_table = 
			--database
			COALESCE(concat('[', case when trim(split_part(V_destination_table, '.', -3)) = '' or split_part(V_destination_table, '.', -3) is null then null else split_part(V_destination_table, '.', -3) end, ']') || '.', '') +
			--schema
			COALESCE(concat('[', case when trim(split_part(V_destination_table, '.', -2)) = '' or split_part(V_destination_table, '.', -2) is null then null else split_part(V_destination_table, '.', -2) end, ']') || '.', '') +
			--table
			COALESCE(concat('[', case when trim(split_part(V_destination_table, '.', -1)) = '' or split_part(V_destination_table, '.', -1) is null then null else split_part(V_destination_table, '.', -1) end, ']'), '');
IF COALESCE(RTRIM(V_destination_table), '') = ''
		THEN
RESIGNAL;
SIGNAL SQLSTATE '45000';
END IF;
END IF;
SET (V_sort_order) = (
WITH
	a0 AS
	(SELECT 1 AS n UNION ALL SELECT 1),
	a1 AS
	(SELECT 1 AS n FROM a0 AS a, a0 AS b),
	a2 AS
	(SELECT 1 AS n FROM a1 AS a, a1 AS b),
	a3 AS
	(SELECT 1 AS n FROM a2 AS a, a2 AS b),
	a4 AS
	(SELECT 1 AS n FROM a3 AS a, a3 AS b),
	numbers AS
	(
		SELECT TOP(LEN(V_sort_order))
			ROW_NUMBER() OVER
			(
				ORDER BY (SELECT NULL)
			) AS BIGINT
		FROM a4
		ORDER BY
			BIGINT
	),
	tokens AS
	(
		SELECT 
			'|`' ||
				SUBSTRING
				(
					V_sort_order,
					BIGINT + 1,
					INSTR(SUBSTRING(V_sort_order, BIGINT), '`') - BIGINT - 1
				) || '|`' AS token,
			SUBSTRING
			(
				V_sort_order,
				INSTR(SUBSTRING(V_sort_order, BIGINT), '`') + 1,
				COALESCE(NULLIF(INSTR(SUBSTRING(V_sort_order, INSTR(SUBSTRING(V_sort_order, BIGINT), '`')), '`'), 0), LEN(V_sort_order)) - INSTR(SUBSTRING(V_sort_order, BIGINT), '`')
			) AS next_chunk,
			BIGINT
		FROM numbers
		WHERE
			SUBSTRING(V_sort_order, BIGINT, 1) = '`'
	),
	ordered_columns AS
	(
		SELECT
			x.column_name +
				CASE
					WHEN tokens.next_chunk LIKE '%asc%' THEN ' ASC'
					WHEN tokens.next_chunk LIKE '%desc%' THEN ' DESC'
					ELSE ''
				END AS column_name,
			ROW_NUMBER() OVER
			(
				PARTITION BY
					x.column_name
				ORDER BY
					tokens.number
			) AS r,
			tokens.number
		FROM tokens
		JOIN
		(
			SELECT '`session_id`' AS column_name
			UNION ALL
			SELECT '`physical_io`'
			UNION ALL
			SELECT '`reads`'
			UNION ALL
			SELECT '`physical_reads`'
			UNION ALL
			SELECT '`writes`'
			UNION ALL
			SELECT '`tempdb_allocations`'
			UNION ALL
			SELECT '`tempdb_current`'
			UNION ALL
			SELECT '`CPU`'
			UNION ALL
			SELECT '`context_switches`'
			UNION ALL
			SELECT '`used_memory`'
			UNION ALL
			SELECT '`physical_io_delta`'
			UNION ALL
			SELECT '`reads_delta`'
			UNION ALL
			SELECT '`physical_reads_delta`'
			UNION ALL
			SELECT '`writes_delta`'
			UNION ALL
			SELECT '`tempdb_allocations_delta`'
			UNION ALL
			SELECT '`tempdb_current_delta`'
			UNION ALL
			SELECT '`CPU_delta`'
			UNION ALL
			SELECT '`context_switches_delta`'
			UNION ALL
			SELECT '`used_memory_delta`'
			UNION ALL
			SELECT '`tasks`'
			UNION ALL
			SELECT '`tran_start_time`'
			UNION ALL
			SELECT '`open_tran_count`'
			UNION ALL
			SELECT '`blocking_session_id`'
			UNION ALL
			SELECT '`blocked_session_count`'
			UNION ALL
			SELECT '`percent_complete`'
			UNION ALL
			SELECT '`host_name`'
			UNION ALL
			SELECT '`login_name`'
			UNION ALL
			SELECT '`database_name`'
			UNION ALL
			SELECT '`start_time`'
			UNION ALL
			SELECT '`login_time`'
			UNION ALL
			SELECT '`program_name`'
		) AS x ON 
			x.column_name LIKE token ESCAPE '|'
	)
SELECT
		COALESCE(z.sort_order, '')
	FROM
	(
		SELECT
			CONCAT(substring((
SELECT
',' || column_name as `text__`
FROM ordered_columns
WHERE
r = 1
ORDER BY
BIGINT
FOR XML
PATH('')
), 1, 1 - 1), '', substring((
SELECT
',' || column_name as `text__`
FROM ordered_columns
WHERE
r = 1
ORDER BY
BIGINT
FOR XML
PATH('')
), 1 + 1)) AS sort_order
	) AS z;
);
CREATE TEMPORARY TABLE TEMP_TABLE_sessions
	(
		recursion SMALLINT NOT NULL,
		session_id SMALLINT NOT NULL,
		request_id INT NOT NULL,
		session_number INT NOT NULL,
		elapsed_time INT NOT NULL,
		avg_elapsed_time INT,
		physical_io BIGINT,
		reads BIGINT,
		physical_reads BIGINT,
		writes BIGINT,
		tempdb_allocations BIGINT,
		tempdb_current BIGINT,
		CPU INT,
		thread_CPU_snapshot BIGINT,
		context_switches BIGINT,
		used_memory BIGINT NOT NULL, 
		tasks SMALLINT ,
		status STRING NOT NULL,
		wait_info STRING,
		locks STRING ,
		transaction_id BIGINT,
		tran_start_time TIMESTAMP ,
		tran_log_writes STRING,
		open_tran_count SMALLINT ,
		sql_command STRING ,
		sql_handle BINARY ,
		statement_start_offset INT,
		statement_end_offset INT,
		sql_text STRING ,
		plan_handle BINARY ,
		query_plan STRING ,
		blocking_session_id SMALLINT ,
		blocked_session_count SMALLINT ,
		percent_complete FLOAT,
		host_name string ,
		login_name string NOT NULL,
		database_name string ,
		program_name string ,
		additional_info STRING ,
		start_time TIMESTAMP NOT NULL,
		login_time TIMESTAMP ,
		last_request_start_time TIMESTAMP ,
		PRIMARY KEY CLUSTERED (session_id, request_id, recursion) WITH(IGNORE_DUP_KEY = ON),
		UNIQUE NONCLUSTERED (transaction_id, session_id, request_id, recursion) WITH(IGNORE_DUP_KEY = ON)
	);
IF V_return_schema = 0
	THEN
		--Disable unnecessary autostats on the table
		
WITH SAMPLE 0 ROWS, NORECOMPUTE;
WITH SAMPLE 0 ROWS, NORECOMPUTE;
WITH SAMPLE 0 ROWS, NORECOMPUTE;
WITH SAMPLE 0 ROWS, NORECOMPUTE;
WITH SAMPLE 0 ROWS, NORECOMPUTE;
WITH SAMPLE 0 ROWS, NORECOMPUTE;
WITH SAMPLE 0 ROWS, NORECOMPUTE;
WITH SAMPLE 0 ROWS, NORECOMPUTE;
SET V_recursion = 
			CASE V_delta_interval
				WHEN 0 THEN 1
				ELSE -1
			END;
SET V_sys_info = COALESCE(CAST(SIGN(OBJECT_ID('sys.dm_os_sys_info')) AS BOOLEAN), 0);
		--Used for the delta pull
		REDO:;
IF 
			V_get_locks = 1 
			AND V_recursion = 1
			AND V_output_column_list LIKE '%|`locks|`%' ESCAPE '|'
		THEN

			CREATE TEMPORARY TABLE TEMP_TABLE_locks AS
SELECT
				y.resource_type,
				y.database_name,
				y.object_id,
				y.file_id,
				y.page_type,
				y.hobt_id,
				y.allocation_unit_id,
				y.index_id,
				y.schema_id,
				y.principal_id,
				y.request_mode,
				y.request_status,
				y.session_id,
				y.resource_description,
				y.request_count,
				s.request_id,
				s.start_time,
				CAST(NULL AS string) AS object_name,
				CAST(NULL AS string) AS index_name,
				CAST(NULL AS string) AS schema_name,
				CAST(NULL AS string) AS principal_name,
				CAST(NULL AS STRING) AS query_error
			
			FROM
			(
				SELECT
					sp.spid AS session_id,
					CASE sp.status
						WHEN 'sleeping' THEN CAST(0 AS INT)
						ELSE sp.request_id
					END AS request_id,
					CASE sp.status
						WHEN 'sleeping' THEN sp.last_batch
						ELSE COALESCE(req.start_time, sp.last_batch)
					END AS start_time,
					sp.dbid
				FROM sys.sysprocesses AS sp
				LEFT JOIN LATERAL
				(
					SELECT TOP(1)
						CASE
							WHEN 
							(
								sp.hostprocess > ''
								OR r.total_elapsed_time < 0
							) THEN
								r.start_time
							ELSE
								DATEADD(MILLISECOND, 1000 * (EXTRACT(ms from DATEADD(second, -(r.total_elapsed_time / 1000), current_timestamp())) / 500) - EXTRACT(ms from DATEADD(second, -(r.total_elapsed_time / 1000), current_timestamp())), DATEADD(second, -(r.total_elapsed_time / 1000), current_timestamp()))
						END AS start_time
					FROM sys.dm_exec_requests AS r
					WHERE
						r.session_id = sp.spid
						AND r.request_id = sp.request_id
				) AS req
				WHERE
					--Process inclusive filter
					1 =
						CASE
							WHEN V_filter <> '' THEN
								CASE V_filter_type
									WHEN 'session' THEN
										CASE
											WHEN
												CAST(V_filter AS SMALLINT) = 0
												OR sp.spid = CAST(V_filter AS SMALLINT)
													THEN 1
											ELSE 0
										END
									WHEN 'program' THEN
										CASE
											WHEN sp.program_name LIKE V_filter THEN 1
											ELSE 0
										END
									WHEN 'login' THEN
										CASE
											WHEN sp.loginame LIKE V_filter THEN 1
											ELSE 0
										END
									WHEN 'host' THEN
										CASE
											WHEN sp.hostname LIKE V_filter THEN 1
											ELSE 0
										END
									WHEN 'database' THEN
										CASE
											WHEN current_database() LIKE V_filter THEN 1
											ELSE 0
										END
									ELSE 0
								END
							ELSE 1
						END
					--Process exclusive filter
					AND 0 =
						CASE
							WHEN V_not_filter <> '' THEN
								CASE V_not_filter_type
									WHEN 'session' THEN
										CASE
											WHEN sp.spid = CAST(V_not_filter AS SMALLINT) THEN 1
											ELSE 0
										END
									WHEN 'program' THEN
										CASE
											WHEN sp.program_name LIKE V_not_filter THEN 1
											ELSE 0
										END
									WHEN 'login' THEN
										CASE
											WHEN sp.loginame LIKE V_not_filter THEN 1
											ELSE 0
										END
									WHEN 'host' THEN
										CASE
											WHEN sp.hostname LIKE V_not_filter THEN 1
											ELSE 0
										END
									WHEN 'database' THEN
										CASE
											WHEN current_database() LIKE V_not_filter THEN 1
											ELSE 0
										END
									ELSE 0
								END
							ELSE 0
						END
					AND 
					(
						V_show_own_spid = 1
						OR sp.spid <> V_V_SPID
					)
					AND 
					(
						V_show_system_spids = 1
						OR sp.hostprocess > ''
					)
					AND sp.ecid = 0
			) AS s
			INNER JOIN
			(
				SELECT
					x.resource_type,
					x.database_name,
					x.object_id,
					x.file_id,
					CASE
						WHEN x.page_no = 1 OR x.page_no % 8088 = 0 THEN 'PFS'
						WHEN x.page_no = 2 OR x.page_no % 511232 = 0 THEN 'GAM'
						WHEN x.page_no = 3 OR (x.page_no - 1) % 511232 = 0 THEN 'SGAM'
						WHEN x.page_no = 6 OR (x.page_no - 6) % 511232 = 0 THEN 'DCM'
						WHEN x.page_no = 7 OR (x.page_no - 7) % 511232 = 0 THEN 'BCM'
						WHEN x.page_no IS NOT NULL THEN '*'
						ELSE NULL
					END AS page_type,
					x.hobt_id,
					x.allocation_unit_id,
					x.index_id,
					x.schema_id,
					x.principal_id,
					x.request_mode,
					x.request_status,
					x.session_id,
					x.request_id,
					CASE
						WHEN COALESCE(x.object_id, x.file_id, x.hobt_id, x.allocation_unit_id, x.index_id, x.schema_id, x.principal_id) IS NULL THEN NULLIF(resource_description, '')
						ELSE NULL
					END AS resource_description,
					COUNT(*) AS request_count
				FROM
				(
					SELECT
						tl.resource_type +
							CASE
								WHEN tl.resource_subtype = '' THEN ''
								ELSE '.' + tl.resource_subtype
							END AS resource_type,
						COALESCE(current_database(), '(null)') AS database_name,
						CAST(CASE
WHEN tl.resource_type = 'OBJECT' THEN tl.resource_associated_entity_id
WHEN tl.resource_description LIKE '%object_id = %' THEN
(
SUBSTRING
(
tl.resource_description, 
(INSTR(tl.resource_description, 'object_id = ') + 12), 
COALESCE
(
NULLIF
(
INSTR(SUBSTRING(tl.resource_description, INSTR(tl.resource_description, 'object_id = ') + 12), ','),
0
), 
DATALENGTH(tl.resource_description)+1
) - (INSTR(tl.resource_description, 'object_id = ') + 12)
)
)
ELSE NULL
END AS INT) AS object_id,
						CAST(CASE 
WHEN tl.resource_type = 'FILE' THEN CAST(tl.resource_description AS INT)
WHEN tl.resource_type IN ('PAGE', 'EXTENT', 'RID') THEN LEFT(tl.resource_description, INSTR(tl.resource_description, ':')-1)
ELSE NULL
END AS INT) AS file_id,
						CAST(CASE
WHEN tl.resource_type IN ('PAGE', 'EXTENT', 'RID') THEN 
SUBSTRING
(
tl.resource_description, 
INSTR(tl.resource_description, ':') + 1, 
COALESCE
(
NULLIF
(
INSTR(SUBSTRING(tl.resource_description, INSTR(tl.resource_description, ':') + 1), ':'), 
0
), 
DATALENGTH(tl.resource_description)+1
) - (INSTR(tl.resource_description, ':') + 1)
)
ELSE NULL
END AS INT) AS page_no,
						CASE
							WHEN tl.resource_type IN ('PAGE', 'KEY', 'RID', 'HOBT') THEN tl.resource_associated_entity_id
							ELSE NULL
						END AS hobt_id,
						CASE
							WHEN tl.resource_type = 'ALLOCATION_UNIT' THEN tl.resource_associated_entity_id
							ELSE NULL
						END AS allocation_unit_id,
						CAST(CASE
WHEN
/*TODO: Deal with server principals*/ 
tl.resource_subtype <> 'SERVER_PRINCIPAL' 
AND tl.resource_description LIKE '%index_id or stats_id = %' THEN
(
SUBSTRING
(
tl.resource_description, 
(INSTR(tl.resource_description, 'index_id or stats_id = ') + 23), 
COALESCE
(
NULLIF
(
INSTR(SUBSTRING(tl.resource_description, INSTR(tl.resource_description, 'index_id or stats_id = ') + 23), ','), 
0
), 
DATALENGTH(tl.resource_description)+1
) - (INSTR(tl.resource_description, 'index_id or stats_id = ') + 23)
)
)
ELSE NULL
END AS INT) AS index_id,
						CAST(CASE
WHEN tl.resource_description LIKE '%schema_id = %' THEN
(
SUBSTRING
(
tl.resource_description, 
(INSTR(tl.resource_description, 'schema_id = ') + 12), 
COALESCE
(
NULLIF
(
INSTR(SUBSTRING(tl.resource_description, INSTR(tl.resource_description, 'schema_id = ') + 12), ','), 
0
), 
DATALENGTH(tl.resource_description)+1
) - (INSTR(tl.resource_description, 'schema_id = ') + 12)
)
)
ELSE NULL
END AS INT) AS schema_id,
						CAST(CASE
WHEN tl.resource_description LIKE '%principal_id = %' THEN
(
SUBSTRING
(
tl.resource_description, 
(INSTR(tl.resource_description, 'principal_id = ') + 15), 
COALESCE
(
NULLIF
(
INSTR(SUBSTRING(tl.resource_description, INSTR(tl.resource_description, 'principal_id = ') + 15), ','), 
0
), 
DATALENGTH(tl.resource_description)+1
) - (INSTR(tl.resource_description, 'principal_id = ') + 15)
)
)
ELSE NULL
END AS INT) AS principal_id,
						tl.request_mode,
						tl.request_status,
						tl.request_session_id AS session_id,
						tl.request_request_id AS request_id,

						/*TODO: Applocks, other resource_descriptions*/
						RTRIM(tl.resource_description) AS resource_description,
						tl.resource_associated_entity_id
						/*********************************************/
					FROM 
					(
						SELECT 
							request_session_id,
							CAST(resource_type AS STRING) COLLATE Latin1_General_Bin2 AS resource_type,
							CAST(resource_subtype AS STRING) COLLATE Latin1_General_Bin2 AS resource_subtype,
							resource_database_id,
							CAST(resource_description AS STRING) COLLATE Latin1_General_Bin2 AS resource_description,
							resource_associated_entity_id,
							CAST(request_mode AS STRING) COLLATE Latin1_General_Bin2 AS request_mode,
							CAST(request_status AS STRING) COLLATE Latin1_General_Bin2 AS request_status,
							request_request_id
						FROM sys.dm_tran_locks
					) AS tl
				) AS x
				GROUP BY
					x.resource_type,
					x.database_name,
					x.object_id,
					x.file_id,
					CASE
						WHEN x.page_no = 1 OR x.page_no % 8088 = 0 THEN 'PFS'
						WHEN x.page_no = 2 OR x.page_no % 511232 = 0 THEN 'GAM'
						WHEN x.page_no = 3 OR (x.page_no - 1) % 511232 = 0 THEN 'SGAM'
						WHEN x.page_no = 6 OR (x.page_no - 6) % 511232 = 0 THEN 'DCM'
						WHEN x.page_no = 7 OR (x.page_no - 7) % 511232 = 0 THEN 'BCM'
						WHEN x.page_no IS NOT NULL THEN '*'
						ELSE NULL
					END,
					x.hobt_id,
					x.allocation_unit_id,
					x.index_id,
					x.schema_id,
					x.principal_id,
					x.request_mode,
					x.request_status,
					x.session_id,
					x.request_id,
					CASE
						WHEN COALESCE(x.object_id, x.file_id, x.hobt_id, x.allocation_unit_id, x.index_id, x.schema_id, x.principal_id) IS NULL THEN NULLIF(resource_description, '')
						ELSE NULL
					END
			) AS y ON
				y.session_id = s.session_id
				AND y.request_id = s.request_id
			;
			--Disable unnecessary autostats on the table
			
WITH SAMPLE 0 ROWS, NORECOMPUTE;
WITH SAMPLE 0 ROWS, NORECOMPUTE;
WITH SAMPLE 0 ROWS, NORECOMPUTE;
WITH SAMPLE 0 ROWS, NORECOMPUTE;
WITH SAMPLE 0 ROWS, NORECOMPUTE;
WITH SAMPLE 0 ROWS, NORECOMPUTE;
WITH SAMPLE 0 ROWS, NORECOMPUTE;
WITH SAMPLE 0 ROWS, NORECOMPUTE;
WITH SAMPLE 0 ROWS, NORECOMPUTE;
WITH SAMPLE 0 ROWS, NORECOMPUTE;
WITH SAMPLE 0 ROWS, NORECOMPUTE;
WITH SAMPLE 0 ROWS, NORECOMPUTE;
WITH SAMPLE 0 ROWS, NORECOMPUTE;
WITH SAMPLE 0 ROWS, NORECOMPUTE;
WITH SAMPLE 0 ROWS, NORECOMPUTE;
WITH SAMPLE 0 ROWS, NORECOMPUTE;
WITH SAMPLE 0 ROWS, NORECOMPUTE;
WITH SAMPLE 0 ROWS, NORECOMPUTE;
END IF;
SET V_sql = 
			CAST('' AS STRING) ||
			'DECLARE V_blocker BOOLEAN;
			SET V_blocker = 0;
			DECLARE V_i INT;
			SET V_i = 2147483647;

			DECLARE V_sessions TABLE
			(
				session_id SMALLINT NOT NULL,
				request_id INT NOT NULL,
				login_time TIMESTAMP,
				last_request_end_time TIMESTAMP,
				status STRING,
				statement_start_offset INT,
				statement_end_offset INT,
				sql_handle BINARY,
				host_name STRING,
				login_name STRING,
				program_name STRING,
				database_id SMALLINT,
				memory_usage INT,
				open_tran_count SMALLINT, 
				' ||
				CASE
					WHEN 
					(
						V_get_task_info <> 0 
						OR V_find_block_leaders = 1 
					) THEN
						'wait_type NVARCHAR(32),
						wait_resource NVARCHAR(256),
						wait_time BIGINT, 
						'
					ELSE 
						''
				END ||
				'blocked SMALLINT,
				is_user_process BOOLEAN,
				cmd STRING,
				PRIMARY KEY CLUSTERED (session_id, request_id) WITH (IGNORE_DUP_KEY = ON)
			);

			DECLARE V_blockers TABLE
			(
				session_id INT NOT NULL PRIMARY KEY WITH (IGNORE_DUP_KEY = ON)
			);

			BLOCKERS:;

			INSERT V_sessions
			(
				session_id,
				request_id,
				login_time,
				last_request_end_time,
				status,
				statement_start_offset,
				statement_end_offset,
				sql_handle,
				host_name,
				login_name,
				program_name,
				database_id,
				memory_usage,
				open_tran_count, 
				' ||
				CASE
					WHEN 
					(
						V_get_task_info <> 0
						OR V_find_block_leaders = 1 
					) THEN
						'wait_type,
						wait_resource,
						wait_time, 
						'
					ELSE
						''
				END ||
				'blocked,
				is_user_process,
				cmd 
			)
			SELECT TOP(V_i)
				spy.session_id,
				spy.request_id,
				spy.login_time,
				spy.last_request_end_time,
				spy.status,
				spy.statement_start_offset,
				spy.statement_end_offset,
				spy.sql_handle,
				spy.host_name,
				spy.login_name,
				spy.program_name,
				spy.database_id,
				spy.memory_usage,
				spy.open_tran_count,
				' ||
				CASE
					WHEN 
					(
						V_get_task_info <> 0  
						OR V_find_block_leaders = 1 
					) THEN
						'spy.wait_type,
						CASE
							WHEN
								spy.wait_type LIKE N"PAGE%LATCH_%"
								OR spy.wait_type "CXPACKET"
								OR spy.wait_type LIKE N"LATCH[_]%"
								OR spy.wait_type "OLEDB" THEN
									spy.wait_resource
							ELSE
								NULL
						END AS wait_resource,
						spy.wait_time, 
						'
					ELSE
						''
				END ||
				'spy.blocked,
				spy.is_user_process,
				spy.cmd
			FROM
			(
				SELECT TOP(V_i)
					spx.*, 
					' ||
					CASE
						WHEN 
						(
							V_get_task_info <> 0 
							OR V_find_block_leaders = 1 
						) THEN
							'ROW_NUMBER() OVER
							(
								PARTITION BY
									spx.session_id,
									spx.request_id
								ORDER BY
									CASE
										WHEN spx.wait_type LIKE N"LCK[_]%" THEN 
											1
										ELSE
											99
									END,
									spx.wait_time DESC,
									spx.blocked DESC
							) AS r 
							'
						ELSE 
							'1 AS r 
							'
					END ||
				'FROM
				(
					SELECT TOP(V_i)
						sp0.session_id,
						sp0.request_id,
						sp0.login_time,
						sp0.last_request_end_time,
						LOWER(sp0.status) AS status,
						CASE
							WHEN sp0.cmd = "CREATE INDEX" THEN
								0
							ELSE
								sp0.stmt_start
						END AS statement_start_offset,
						CASE
							WHEN sp0.cmd "CREATE INDEX" THEN
								-1
							ELSE
								COALESCE(NULLIF(sp0.stmt_end, 0), -1)
						END AS statement_end_offset,
						sp0.sql_handle,
						sp0.host_name,
						sp0.login_name,
						sp0.program_name,
						sp0.database_id,
						sp0.memory_usage,
						sp0.open_tran_count, 
						' ||
						CASE
							WHEN 
							(
								V_get_task_info <> 0 
								OR V_find_block_leaders = 1 
							) THEN
								'CASE
									WHEN sp0.wait_time > 0 AND sp0.wait_type <> N"CXPACKET" THEN
										sp0.wait_type
									ELSE
										NULL
								END AS wait_type,
								CASE
									WHEN sp0.wait_time > 0 AND sp0.wait_type <> N"CXPACKET" THEN 
										sp0.wait_resource
									ELSE
										NULL
								END AS wait_resource,
								CASE
									WHEN sp0.wait_type <> N"CXPACKET" THEN
										sp0.wait_time
									ELSE
										0
								END AS wait_time, 
								'
							ELSE
								''
						END ||
						'sp0.blocked,
						sp0.is_user_process,
						sp0.cmd
					FROM
					(
						SELECT TOP(V_i)
							sp1.session_id,
							sp1.request_id,
							sp1.login_time,
							sp1.last_request_end_time,
							sp1.status,
							sp1.cmd,
							sp1.stmt_start,
							sp1.stmt_end,
							MAX(NULLIF(sp1.sql_handle, 0x00)) OVER (PARTITION BY sp1.session_id, sp1.request_id) AS sql_handle,
							sp1.host_name,
							MAX(sp1.login_name) OVER (PARTITION BY sp1.session_id, sp1.request_id) AS login_name,
							sp1.program_name,
							sp1.database_id,
							MAX(sp1.memory_usage)  OVER (PARTITION BY sp1.session_id, sp1.request_id) AS memory_usage,
							MAX(sp1.open_tran_count)  OVER (PARTITION BY sp1.session_id, sp1.request_id) AS open_tran_count,
							sp1.wait_type,
							sp1.wait_resource,
							sp1.wait_time,
							sp1.blocked,
							sp1.hostprocess,
							sp1.is_user_process
						FROM
						(
							SELECT TOP(V_i)
								sp2.spid AS session_id,
								CASE sp2.status
									WHEN "sleeping" THEN
										CAST(0 AS INT)
									ELSE
										sp2.request_id
								END AS request_id,
								MAX(sp2.login_time) AS login_time,
								MAX(sp2.last_batch) AS last_request_end_time,
								MAX(CAST(RTRIM(sp2.status) AS STRING) COLLATE Latin1_General_Bin2) AS status,
								MAX(CAST(RTRIM(sp2.cmd) AS STRING) COLLATE Latin1_General_Bin2) AS cmd,
								MAX(sp2.stmt_start) AS stmt_start,
								MAX(sp2.stmt_end) AS stmt_end,
								MAX(sp2.sql_handle) AS sql_handle,
								MAX(CAST(RTRIM(sp2.hostname) AS string) COLLATE SQL_Latin1_General_CP1_CI_AS) AS host_name,
								MAX(CAST(RTRIM(sp2.loginame) AS string) COLLATE SQL_Latin1_General_CP1_CI_AS) AS login_name,
								MAX
								(
									CASE
										WHEN blk.queue_id IS NOT NULL THEN
											N"Service Broker
												database_id: "|| CAST(blk.database_id AS STRING) " queue_id: "|| CAST(blk.queue_id AS STRING)
										ELSE
											CAST(RTRIM(sp2.program_name) AS string)
									END COLLATE SQL_Latin1_General_CP1_CI_AS
								) AS program_name,
								MAX(sp2.dbid) AS database_id,
								MAX(sp2.memusage) AS memory_usage,
								MAX(sp2.open_tran) AS open_tran_count,
								RTRIM(sp2.lastwaittype) AS wait_type,
								RTRIM(sp2.waitresource) AS wait_resource,
								MAX(sp2.waittime) AS wait_time,
								COALESCE(NULLIF(sp2.blocked, sp2.spid), 0) AS blocked,
								MAX
								(
									CASE
										WHEN blk.session_id = sp2.spid THEN
											"blocker"
										ELSE
											RTRIM(sp2.hostprocess)
									END
								) AS hostprocess,
								CAST(MAX
(
CASE
WHEN sp2.hostprocess > "" THEN
1
ELSE
0
END
) AS BOOLEAN) AS is_user_process
							FROM
							(
								SELECT TOP(V_i)
									session_id,
									CAST(NULL AS INT) AS queue_id,
									CAST(NULL AS INT) AS database_id
								FROM V_blockers

								UNION ALL

								SELECT TOP(V_i)
									CAST(0 AS SMALLINT),
									CAST(NULL AS INT) AS queue_id,
									CAST(NULL AS INT) AS database_id
								WHERE
									V_blocker = 0

								UNION ALL

								SELECT TOP(V_i)
									CAST(spid AS SMALLINT),
									queue_id,
									database_id
								FROM sys.dm_broker_activated_tasks
								WHERE
									V_blocker = 0
							) AS blk
							INNER JOIN sys.sysprocesses AS sp2 ON
								sp2.spid = blk.session_id
								OR
								(
									blk.session_id = 0
									AND V_blocker = 0
								)
							' ||
							CASE 
								WHEN 
								(
									V_get_task_info = 0 
									AND V_find_block_leaders = 0
								) THEN
									'WHERE
										sp2.ecid = 0 
									' 
								ELSE
									''
							END ||
							'GROUP BY
								sp2.spid,
								CASE sp2.status
									WHEN "sleeping" THEN
										CAST(0 AS INT)
									ELSE
										sp2.request_id
								END,
								RTRIM(sp2.lastwaittype),
								RTRIM(sp2.waitresource),
								COALESCE(NULLIF(sp2.blocked, sp2.spid), 0)
						) AS sp1
					) AS sp0
					WHERE
						V_blocker = 1
						OR
						(1=1 
						' ||
							--inclusive filter
							CASE
								WHEN V_filter <> '' THEN
									CASE V_filter_type
										WHEN 'session' THEN
											CASE
												WHEN CAST(V_filter AS SMALLINT) <> 0 THEN
													'AND sp0.session_id = CONVERT(SMALLINT, V_filter) 
													'
												ELSE
													''
											END
										WHEN 'program' THEN
											'AND sp0.program_name LIKE V_filter 
											'
										WHEN 'login' THEN
											'AND sp0.login_name LIKE V_filter 
											'
										WHEN 'host' THEN
											'AND sp0.host_name LIKE V_filter 
											'
										WHEN 'database' THEN
											'AND DB_NAME(sp0.database_id) LIKE V_filter 
											'
										ELSE
											''
									END
								ELSE
									''
							END +
							--exclusive filter
							CASE
								WHEN V_not_filter <> '' THEN
									CASE V_not_filter_type
										WHEN 'session' THEN
											CASE
												WHEN CAST(V_not_filter AS SMALLINT) <> 0 THEN
													'AND sp0.session_id <> CONVERT(SMALLINT, V_not_filter) 
													'
												ELSE
													''
											END
										WHEN 'program' THEN
											'AND sp0.program_name NOT LIKE V_not_filter 
											'
										WHEN 'login' THEN
											'AND sp0.login_name NOT LIKE V_not_filter 
											'
										WHEN 'host' THEN
											'AND sp0.host_name NOT LIKE V_not_filter 
											'
										WHEN 'database' THEN
											'AND DB_NAME(sp0.database_id) NOT LIKE V_not_filter 
											'
										ELSE
											''
									END
								ELSE
									''
							END +
							CASE V_show_own_spid
								WHEN 1 THEN
									''
								ELSE
									'AND sp0.session_id <> V_V_spid 
									'
							END +
							CASE 
								WHEN V_show_system_spids = 0 THEN
									'AND sp0.hostprocess > "" 
									' 
								ELSE
									''
							END +
							CASE V_show_sleeping_spids
								WHEN 0 THEN
									'AND sp0.status <> "sleeping" 
									'
								WHEN 1 THEN
									'AND
									(
										sp0.status <> "sleeping"
										OR sp0.open_tran_count > 0
									)
									'
								ELSE
									''
							END ||
						')
				) AS spx
			) AS spy
			WHERE
				spy.r = 1; 
			' || 
			CASE V_recursion
				WHEN 1 THEN 
					'IF V_V_ROWCOUNT > 0
					BEGIN

						INSERT V_blockers
						(
							session_id
						)
						SELECT TOP(V_i)
							blocked
						FROM V_sessions
						WHERE
							NULLIF(blocked, 0) IS NOT NULL

						EXCEPT

						SELECT TOP(V_i)
							session_id
						FROM V_sessions; 
						' +

						CASE
							WHEN
							(
								V_get_task_info > 0
								OR V_find_block_leaders = 1
							) THEN
								'IF V_V_ROWCOUNT > 0
								BEGIN

									SET V_blocker = 1;
									GOTO BLOCKERS;
								END; 
								'
							ELSE 
								''
						END +
					'END; 
					'
				ELSE 
					''
			END ||
			'SELECT TOP(V_i)
				V_recursion AS recursion,
				x.session_id,
				x.request_id,
				DENSE_RANK() OVER
				(
					ORDER BY
						x.session_id
				) AS session_number,
				' ||
				CASE
					WHEN V_output_column_list LIKE '%|[dd hh:mm:ss.mss|]%' ESCAPE '|' THEN 
						'x.elapsed_time '
					ELSE 
						'0 '
				END || 
					'AS elapsed_time, 
					' ||
				CASE
					WHEN
						(
							V_output_column_list LIKE '%|[dd hh:mm:ss.mss (avg)|]%' ESCAPE '|' OR 
							V_output_column_list LIKE '%|[avg_elapsed_time|]%' ESCAPE '|'
						)
						AND V_recursion = 1
							THEN 
								'x.avg_elapsed_time / 1000 '
					ELSE 
						'NULL '
				END || 
					'AS avg_elapsed_time, 
					' ||
				CASE
					WHEN 
						V_output_column_list LIKE '%|[physical_io|]%' ESCAPE '|'
						OR V_output_column_list LIKE '%|[physical_io_delta|]%' ESCAPE '|'
							THEN 
								'x.physical_io '
					ELSE 
						'NULL '
				END || 
					'AS physical_io, 
					' ||
				CASE
					WHEN 
						V_output_column_list LIKE '%|[reads|]%' ESCAPE '|'
						OR V_output_column_list LIKE '%|[reads_delta|]%' ESCAPE '|'
							THEN 
								'x.reads '
					ELSE 
						'0 '
				END || 
					'AS reads, 
					' ||
				CASE
					WHEN 
						V_output_column_list LIKE '%|[physical_reads|]%' ESCAPE '|'
						OR V_output_column_list LIKE '%|[physical_reads_delta|]%' ESCAPE '|'
							THEN 
								'x.physical_reads '
					ELSE 
						'0 '
				END || 
					'AS physical_reads, 
					' ||
				CASE
					WHEN 
						V_output_column_list LIKE '%|[writes|]%' ESCAPE '|'
						OR V_output_column_list LIKE '%|[writes_delta|]%' ESCAPE '|'
							THEN 
								'x.writes '
					ELSE 
						'0 '
				END || 
					'AS writes, 
					' ||
				CASE
					WHEN 
						V_output_column_list LIKE '%|[tempdb_allocations|]%' ESCAPE '|'
						OR V_output_column_list LIKE '%|[tempdb_allocations_delta|]%' ESCAPE '|'
							THEN 
								'x.tempdb_allocations '
					ELSE 
						'0 '
				END || 
					'AS tempdb_allocations, 
					' ||
				CASE
					WHEN 
						V_output_column_list LIKE '%|[tempdb_current|]%' ESCAPE '|'
						OR V_output_column_list LIKE '%|[tempdb_current_delta|]%' ESCAPE '|'
							THEN 
								'x.tempdb_current '
					ELSE 
						'0 '
				END || 
					'AS tempdb_current, 
					' ||
				CASE
					WHEN 
						V_output_column_list LIKE '%|[CPU|]%' ESCAPE '|'
						OR V_output_column_list LIKE '%|[CPU_delta|]%' ESCAPE '|'
							THEN
								'x.CPU '
					ELSE
						'0 '
				END || 
					'AS CPU, 
					' ||
				CASE
					WHEN 
						V_output_column_list LIKE '%|[CPU_delta|]%' ESCAPE '|'
						AND V_get_task_info = 2
						AND V_sys_info = 1
							THEN 
								'x.thread_CPU_snapshot '
					ELSE 
						'0 '
				END || 
					'AS thread_CPU_snapshot, 
					' ||
				CASE
					WHEN 
						V_output_column_list LIKE '%|[context_switches|]%' ESCAPE '|'
						OR V_output_column_list LIKE '%|[context_switches_delta|]%' ESCAPE '|'
							THEN 
								'x.context_switches '
					ELSE 
						'NULL '
				END || 
					'AS context_switches, 
					' ||
				CASE
					WHEN 
						V_output_column_list LIKE '%|[used_memory|]%' ESCAPE '|'
						OR V_output_column_list LIKE '%|[used_memory_delta|]%' ESCAPE '|'
							THEN 
								'x.used_memory '
					ELSE 
						'0 '
				END || 
					'AS used_memory, 
					' ||
				CASE
					WHEN 
						V_output_column_list LIKE '%|[tasks|]%' ESCAPE '|'
						AND V_recursion = 1
							THEN 
								'x.tasks '
					ELSE 
						'NULL '
				END || 
					'AS tasks, 
					' ||
				CASE
					WHEN 
						(
							V_output_column_list LIKE '%|[status|]%' ESCAPE '|' 
							OR V_output_column_list LIKE '%|[sql_command|]%' ESCAPE '|'
						)
						AND V_recursion = 1
							THEN 
								'x.status '
					ELSE 
						'"" '
				END || 
					'AS status, 
					' ||
				CASE
					WHEN 
						V_output_column_list LIKE '%|[wait_info|]%' ESCAPE '|' 
						AND V_recursion = 1
							THEN 
								CASE V_get_task_info
									WHEN 2 THEN
										'COALESCE(x.task_wait_info, x.sys_wait_info) '
									ELSE
										'x.sys_wait_info '
								END
					ELSE 
						'NULL '
				END || 
					'AS wait_info, 
					' ||
				CASE
					WHEN 
						(
							V_output_column_list LIKE '%|[tran_start_time|]%' ESCAPE '|' 
							OR V_output_column_list LIKE '%|[tran_log_writes|]%' ESCAPE '|' 
						)
						AND V_recursion = 1
							THEN 
								'x.transaction_id '
					ELSE 
						'NULL '
				END || 
					'AS transaction_id, 
					' ||
				CASE
					WHEN 
						V_output_column_list LIKE '%|[open_tran_count|]%' ESCAPE '|' 
						AND V_recursion = 1
							THEN 
								'x.open_tran_count '
					ELSE 
						'NULL '
				END || 
					'AS open_tran_count, 
					' ||
				CASE
					WHEN 
						V_output_column_list LIKE '%|[sql_text|]%' ESCAPE '|' 
						AND V_recursion = 1
							THEN 
								'x.sql_handle '
					ELSE 
						'NULL '
				END || 
					'AS sql_handle, 
					' ||
				CASE
					WHEN 
						(
							V_output_column_list LIKE '%|[sql_text|]%' ESCAPE '|' 
							OR V_output_column_list LIKE '%|[query_plan|]%' ESCAPE '|' 
						)
						AND V_recursion = 1
							THEN 
								'x.statement_start_offset '
					ELSE 
						'NULL '
				END || 
					'AS statement_start_offset, 
					' ||
				CASE
					WHEN 
						(
							V_output_column_list LIKE '%|[sql_text|]%' ESCAPE '|' 
							OR V_output_column_list LIKE '%|[query_plan|]%' ESCAPE '|' 
						)
						AND V_recursion = 1
							THEN 
								'x.statement_end_offset '
					ELSE 
						'NULL '
				END || 
					'AS statement_end_offset, 
					' ||
				'NULL AS sql_text, 
					' ||
				CASE
					WHEN 
						V_output_column_list LIKE '%|[query_plan|]%' ESCAPE '|' 
						AND V_recursion = 1
							THEN 
								'x.plan_handle '
					ELSE 
						'NULL '
				END || 
					'AS plan_handle, 
					' ||
				CASE
					WHEN 
						V_output_column_list LIKE '%|[blocking_session_id|]%' ESCAPE '|' 
						AND V_recursion = 1
							THEN 
								'NULLIF(x.blocking_session_id, 0) '
					ELSE 
						'NULL '
				END || 
					'AS blocking_session_id, 
					' ||
				CASE
					WHEN 
						V_output_column_list LIKE '%|[percent_complete|]%' ESCAPE '|'
						AND V_recursion = 1
							THEN 
								'x.percent_complete '
					ELSE 
						'NULL '
				END || 
					'AS percent_complete, 
					' ||
				CASE
					WHEN 
						V_output_column_list LIKE '%|[host_name|]%' ESCAPE '|' 
						AND V_recursion = 1
							THEN 
								'x.host_name '
					ELSE 
						'"" '
				END || 
					'AS host_name, 
					' ||
				CASE
					WHEN 
						V_output_column_list LIKE '%|[login_name|]%' ESCAPE '|' 
						AND V_recursion = 1
							THEN 
								'x.login_name '
					ELSE 
						'"" '
				END || 
					'AS login_name, 
					' ||
				CASE
					WHEN 
						V_output_column_list LIKE '%|[database_name|]%' ESCAPE '|' 
						AND V_recursion = 1
							THEN 
								'DB_NAME(x.database_id) '
					ELSE 
						'NULL '
				END || 
					'AS database_name, 
					' ||
				CASE
					WHEN 
						V_output_column_list LIKE '%|[program_name|]%' ESCAPE '|' 
						AND V_recursion = 1
							THEN 
								'x.program_name '
					ELSE 
						'"" '
				END || 
					'AS program_name, 
					' ||
				CASE
					WHEN
						V_output_column_list LIKE '%|[additional_info|]%' ESCAPE '|'
						AND V_recursion = 1
							THEN
								'(
									SELECT TOP(V_i)
										x.text_size,
										x.language,
										x.date_format,
										x.date_first,
										CASE x.quoted_identifier
											WHEN 0 THEN "OFF"
											WHEN 1 THEN "ON"
										END AS quoted_identifier,
										CASE x.arithabort
											WHEN 0 THEN "OFF"
											WHEN 1 THEN "ON"
										END AS arithabort,
										CASE x.ansi_null_dflt_on
											WHEN 0 THEN "OFF"
											WHEN 1 THEN "ON"
										END AS ansi_null_dflt_on,
										CASE x.ansi_defaults
											WHEN 0 THEN "OFF"
											WHEN 1 THEN "ON"
										END AS ansi_defaults,
										CASE x.ansi_warnings
											WHEN 0 THEN "OFF"
											WHEN 1 THEN "ON"
										END AS ansi_warnings,
										CASE x.ansi_padding
											WHEN 0 THEN "OFF"
											WHEN 1 THEN "ON"
										END AS ansi_padding,
										CASE ansi_nulls
											WHEN 0 THEN "OFF"
											WHEN 1 THEN "ON"
										END AS ansi_nulls,
										CASE x.concat_null_yields_null
											WHEN 0 THEN "OFF"
											WHEN 1 THEN "ON"
										END AS concat_null_yields_null,
										CASE x.transaction_isolation_level
											WHEN 0 THEN "Unspecified"
											WHEN 1 THEN "ReadUncomitted"
											WHEN 2 THEN "ReadCommitted"
											WHEN 3 THEN "Repeatable"
											WHEN 4 THEN "Serializable"
											WHEN 5 THEN "Snapshot"
										END AS transaction_isolation_level,
										x.lock_timeout,
										x.deadlock_priority,
										x.row_count,
										x.command_type, 
										' +
										CASE
											WHEN OBJECT_ID('master.dbo.fn_varbintohexstr') IS NOT NULL THEN
												'master.dbo.fn_varbintohexstr(x.sql_handle) AS sql_handle,
												master.dbo.fn_varbintohexstr(x.plan_handle) AS plan_handle,'
											ELSE
												'CONVERT(VARCHAR(256), x.sql_handle, 1) AS sql_handle,
												CONVERT(VARCHAR(256), x.plan_handle, 1) AS plan_handle,'
										END +
										'
										x.statement_start_offset,
										x.statement_end_offset,
										' +
										CASE
											WHEN V_output_column_list LIKE '%|[program_name|]%' ESCAPE '|' THEN
												'(
													SELECT TOP(1)
														CONVERT(uniqueidentifier, CONVERT(XML, "").value("xs:hexBinary( substring(sql:column("agent_info.job_id_string"), 0) )", "binary(16)")) AS job_id,
														agent_info.step_id,
														(
															SELECT TOP(1)
																NULL
															FOR XML
																PATH("job_name"),
																TYPE
														),
														(
															SELECT TOP(1)
																NULL
															FOR XML
																PATH("step_name"),
																TYPE
														)
													FROM
													(
														SELECT TOP(1)
															SUBSTRING(x.program_name, CHARINDEX("0x", x.program_name) + 2, 32) AS job_id_string,
															SUBSTRING(x.program_name, CHARINDEX(": Step ", x.program_name) + 7, CHARINDEX(")", x.program_name, CHARINDEX(": Step ", x.program_name)) - (CHARINDEX(": Step ", x.program_name) + 7)) AS step_id
														WHERE
															x.program_name LIKE N"SQLAgent - TSQL JobStep (Job 0x%"
													) AS agent_info
													FOR XML
														PATH("agent_job_info"),
														TYPE
												),
												'
											ELSE ''
										END +
										CASE
											WHEN V_get_task_info = 2 THEN
												'CONVERT(XML, x.block_info) AS block_info, 
												'
											ELSE
												''
										END + '
										x.host_process_id,
										x.group_id
									FOR XML
										PATH("additional_info"),
										TYPE
								) '
					ELSE
						'NULL '
				END || 
					'AS additional_info, 
				x.start_time, 
					' ||
				CASE
					WHEN
						V_output_column_list LIKE '%|[login_time|]%' ESCAPE '|'
						AND V_recursion = 1
							THEN
								'x.login_time '
					ELSE 
						'NULL '
				END || 
					'AS login_time, 
				x.last_request_start_time
			FROM
			(
				SELECT TOP(V_i)
					y.*,
					CASE
						WHEN DATEDIFF(hour, current_timestamp(), y.start_time) > 576 THEN
							DATEDIFF(second, y.start_time, current_timestamp())
						ELSE DATEDIFF(MILLISECOND, current_timestamp(), y.start_time)
					END AS elapsed_time,
					COALESCE(tempdb_info.tempdb_allocations, 0) AS tempdb_allocations,
					COALESCE
					(
						CASE
							WHEN tempdb_info.tempdb_current < 0 THEN 0
							ELSE tempdb_info.tempdb_current
						END,
						0
					) AS tempdb_current, 
					' ||
					CASE
						WHEN 
							(
								V_get_task_info <> 0
								OR V_find_block_leaders = 1
							) THEN
								'N"("|| CONVERT(NVARCHAR, y.wait_duration_ms) "ms)"||
									y.wait_type +
										CASE
											WHEN y.wait_type LIKE N"PAGE%LATCH_%" THEN
												N":"||
												COALESCE(DB_NAME(CONVERT(INT, LEFT(y.resource_description, CHARINDEX(N":", y.resource_description) - 1))), N"(null)") ":"||
												SUBSTRING(y.resource_description, CHARINDEX(N":", y.resource_description) + 1, LEN(y.resource_description) - CHARINDEX(N":", REVERSE(y.resource_description)) - CHARINDEX(N":", y.resource_description)) "("||
													CASE
														WHEN
															CONVERT(INT, RIGHT(y.resource_description, CHARINDEX(N":", REVERSE(y.resource_description)) - 1)) = 1 OR
															CONVERT(INT, RIGHT(y.resource_description, CHARINDEX(N":", REVERSE(y.resource_description)) - 1)) % 8088 = 0
																THEN 
																	N"PFS"
														WHEN
															CONVERT(INT, RIGHT(y.resource_description, CHARINDEX(N":", REVERSE(y.resource_description)) - 1)) = 2 OR
															CONVERT(INT, RIGHT(y.resource_description, CHARINDEX(N":", REVERSE(y.resource_description)) - 1)) % 511232 = 0
																THEN 
																	N"GAM"
														WHEN
															CONVERT(INT, RIGHT(y.resource_description, CHARINDEX(N":", REVERSE(y.resource_description)) - 1)) = 3 OR
															(CONVERT(INT, RIGHT(y.resource_description, CHARINDEX(N":", REVERSE(y.resource_description)) - 1)) - 1) % 511232 = 0
																THEN
																	N"SGAM"
														WHEN
															CONVERT(INT, RIGHT(y.resource_description, CHARINDEX(N":", REVERSE(y.resource_description)) - 1)) = 6 OR
															(CONVERT(INT, RIGHT(y.resource_description, CHARINDEX(N":", REVERSE(y.resource_description)) - 1)) - 6) % 511232 = 0 
																THEN 
																	N"DCM"
														WHEN
															CONVERT(INT, RIGHT(y.resource_description, CHARINDEX(N":", REVERSE(y.resource_description)) - 1)) = 7 OR
															(CONVERT(INT, RIGHT(y.resource_description, CHARINDEX(N":", REVERSE(y.resource_description)) - 1)) - 7) % 511232 = 0 
																THEN 
																	N"BCM"
														ELSE 
															N"*"
													END ")"
											WHEN y.wait_type "CXPACKET" THEN
												N":"|| SUBSTRING(y.resource_description, CHARINDEX(N"nodeId", y.resource_description) + 7, 4)
											WHEN y.wait_type LIKE N"LATCH[_]%" THEN
												N" ["|| LEFT(y.resource_description, COALESCE(NULLIF(CHARINDEX(N" ", y.resource_description), 0), LEN(y.resource_description) + 1) - 1) "]"
											WHEN
												y.wait_type "OLEDB"
												AND y.resource_description LIKE N"%(SPID=%)" THEN
													N"["|| LEFT(y.resource_description, CHARINDEX(N"(SPID=", y.resource_description) - 2) ":"|| SUBSTRING(y.resource_description, CHARINDEX(N"(SPID=", y.resource_description) + 6, CHARINDEX(N")", y.resource_description, (CHARINDEX(N"(SPID=", y.resource_description) + 6)) - (CHARINDEX(N"(SPID=", y.resource_description) + 6)) || "]"
											ELSE
												N""
										END COLLATE Latin1_General_Bin2 AS sys_wait_info, 
										'
							ELSE
								''
						END +
						CASE
							WHEN V_get_task_info = 2 THEN
								'tasks.physical_io,
								tasks.context_switches,
								tasks.tasks,
								tasks.block_info,
								tasks.wait_info AS task_wait_info,
								tasks.thread_CPU_snapshot,
								'
							ELSE
								'' 
					END +
					CASE 
						WHEN NOT (V_get_avg_time = 1 AND V_recursion = 1) THEN
							'CONVERT(INT, NULL) '
						ELSE 
							'qs.total_elapsed_time / qs.execution_count '
					END || 
						'AS avg_elapsed_time 
				FROM
				(
					SELECT TOP(V_i)
						sp.session_id,
						sp.request_id,
						COALESCE(r.logical_reads, s.logical_reads) AS reads,
						COALESCE(r.reads, s.reads) AS physical_reads,
						COALESCE(r.writes, s.writes) AS writes,
						COALESCE(r.CPU_time, s.CPU_time) AS CPU,
						sp.memory_usage + COALESCE(r.granted_query_memory, 0) AS used_memory,
						LOWER(sp.status) AS status,
						COALESCE(r.sql_handle, sp.sql_handle) AS sql_handle,
						COALESCE(r.statement_start_offset, sp.statement_start_offset) AS statement_start_offset,
						COALESCE(r.statement_end_offset, sp.statement_end_offset) AS statement_end_offset,
						' ||
						CASE
							WHEN 
							(
								V_get_task_info <> 0
								OR V_find_block_leaders = 1 
							) THEN
								'sp.wait_type COLLATE Latin1_General_Bin2 AS wait_type,
								sp.wait_resource COLLATE Latin1_General_Bin2 AS resource_description,
								sp.wait_time AS wait_duration_ms, 
								'
							ELSE
								''
						END ||
						'NULLIF(sp.blocked, 0) AS blocking_session_id,
						r.plan_handle,
						NULLIF(r.percent_complete, 0) AS percent_complete,
						sp.host_name,
						sp.login_name,
						sp.program_name,
						s.host_process_id,
						COALESCE(r.text_size, s.text_size) AS text_size,
						COALESCE(r.language, s.language) AS language,
						COALESCE(r.date_format, s.date_format) AS date_format,
						COALESCE(r.date_first, s.date_first) AS date_first,
						COALESCE(r.quoted_identifier, s.quoted_identifier) AS quoted_identifier,
						COALESCE(r.arithabort, s.arithabort) AS arithabort,
						COALESCE(r.ansi_null_dflt_on, s.ansi_null_dflt_on) AS ansi_null_dflt_on,
						COALESCE(r.ansi_defaults, s.ansi_defaults) AS ansi_defaults,
						COALESCE(r.ansi_warnings, s.ansi_warnings) AS ansi_warnings,
						COALESCE(r.ansi_padding, s.ansi_padding) AS ansi_padding,
						COALESCE(r.ansi_nulls, s.ansi_nulls) AS ansi_nulls,
						COALESCE(r.concat_null_yields_null, s.concat_null_yields_null) AS concat_null_yields_null,
						COALESCE(r.transaction_isolation_level, s.transaction_isolation_level) AS transaction_isolation_level,
						COALESCE(r.lock_timeout, s.lock_timeout) AS lock_timeout,
						COALESCE(r.deadlock_priority, s.deadlock_priority) AS deadlock_priority,
						COALESCE(r.row_count, s.row_count) AS row_count,
						COALESCE(r.command, sp.cmd) AS command_type,
						COALESCE
						(
							CASE
								WHEN
								(
									s.is_user_process = 0
									AND r.total_elapsed_time >= 0
								) THEN
									DATEADD(MILLISECOND, 1000 * (EXTRACT(ms from DATEADD(second, -(r.total_elapsed_time / 1000), current_timestamp())) / 500) - EXTRACT(ms from DATEADD(second, -(r.total_elapsed_time / 1000), current_timestamp())), DATEADD(second, -(r.total_elapsed_time / 1000), current_timestamp()))
							END,
							NULLIF(COALESCE(r.start_time, sp.last_request_end_time), date_format("19000101", 'yyyyMMdd')),
							sp.login_time
						) AS start_time,
						sp.login_time,
						CASE
							WHEN s.is_user_process = 1 THEN
								s.last_request_start_time
							ELSE
								COALESCE
								(
									DATEADD(MILLISECOND, 1000 * (EXTRACT(ms from DATEADD(second, -(r.total_elapsed_time / 1000), current_timestamp())) / 500) - EXTRACT(ms from DATEADD(second, -(r.total_elapsed_time / 1000), current_timestamp())), DATEADD(second, -(r.total_elapsed_time / 1000), current_timestamp())),
									s.last_request_start_time
								)
						END AS last_request_start_time,
						r.transaction_id,
						sp.database_id,
						sp.open_tran_count,
						' ||
							CASE
								WHEN EXISTS
								(
									SELECT
										*
									FROM sys.all_columns AS ac
									WHERE
										ac.object_id = OBJECT_ID('sys.dm_exec_sessions')
										AND ac.name = 'group_id'
								)
									THEN 's.group_id'
								ELSE 'CONVERT(INT, NULL) AS group_id'
							END || '
					FROM V_sessions AS sp
					LEFT OUTER LOOP JOIN sys.dm_exec_sessions AS s ON
						s.session_id = sp.session_id
						AND s.login_time = sp.login_time
					LEFT OUTER LOOP JOIN sys.dm_exec_requests AS r ON
						sp.status <> "sleeping"
						AND r.session_id = sp.session_id
						AND r.request_id = sp.request_id
						AND
						(
							(
								s.is_user_process = 0
								AND sp.is_user_process = 0
							)
							OR
							(
								r.start_time = s.last_request_start_time
								AND s.last_request_end_time <= sp.last_request_end_time
							)
						)
				) AS y
				' || 
				CASE 
					WHEN V_get_task_info = 2 THEN
						CAST('' AS STRING) +
						'LEFT OUTER HASH JOIN
						(
							SELECT TOP(V_i)
								task_nodes.task_node.value("(session_id/text())[1]", "SMALLINT") AS session_id,
								task_nodes.task_node.value("(request_id/text())[1]", "INT") AS request_id,
								task_nodes.task_node.value("(physical_io/text())[1]", "BIGINT") AS physical_io,
								task_nodes.task_node.value("(context_switches/text())[1]", "BIGINT") AS context_switches,
								task_nodes.task_node.value("(tasks/text())[1]", "INT") AS tasks,
								task_nodes.task_node.value("(block_info/text())[1]", "NVARCHAR(4000)") AS block_info,
								task_nodes.task_node.value("(waits/text())[1]", "NVARCHAR(4000)") AS wait_info,
								task_nodes.task_node.value("(thread_CPU_snapshot/text())[1]", "BIGINT") AS thread_CPU_snapshot
							FROM
							(
								SELECT TOP(V_i)
									CONVERT
									(
										XML,
										REPLACE
										(
											CONVERT(NVARCHAR(MAX), tasks_raw.task_xml_raw) COLLATE Latin1_General_Bin2,
											N"</waits></tasks><tasks><waits>",
											N", "
										)
									) AS task_xml
								FROM
								(
									SELECT TOP(V_i)
										CASE waits.r
											WHEN 1 THEN
												waits.session_id
											ELSE
												NULL
										END AS [session_id],
										CASE waits.r
											WHEN 1 THEN
												waits.request_id
											ELSE
												NULL
										END AS [request_id],											
										CASE waits.r
											WHEN 1 THEN
												waits.physical_io
											ELSE
												NULL
										END AS [physical_io],
										CASE waits.r
											WHEN 1 THEN
												waits.context_switches
											ELSE
												NULL
										END AS [context_switches],
										CASE waits.r
											WHEN 1 THEN
												waits.thread_CPU_snapshot
											ELSE
												NULL
										END AS [thread_CPU_snapshot],
										CASE waits.r
											WHEN 1 THEN
												waits.tasks
											ELSE
												NULL
										END AS [tasks],
										CASE waits.r
											WHEN 1 THEN
												waits.block_info
											ELSE
												NULL
										END AS [block_info],
										REPLACE
										(
											REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
											REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
											REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
												CONVERT
												(
													NVARCHAR(MAX),
													N"("||
														CONVERT(NVARCHAR, num_waits) "x: "||
														CASE num_waits
															WHEN 1 THEN
																CONVERT(NVARCHAR, min_wait_time) "ms"
															WHEN 2 THEN
																CASE
																	WHEN min_wait_time <> max_wait_time THEN
																		CONVERT(NVARCHAR, min_wait_time) "/"|| CONVERT(NVARCHAR, max_wait_time) "ms"
																	ELSE
																		CONVERT(NVARCHAR, max_wait_time) "ms"
																END
															ELSE
																CASE
																	WHEN min_wait_time <> max_wait_time THEN
																		CONVERT(NVARCHAR, min_wait_time) "/"|| CONVERT(NVARCHAR, avg_wait_time) "/"|| CONVERT(NVARCHAR, max_wait_time) "ms"
																	ELSE 
																		CONVERT(NVARCHAR, max_wait_time) "ms"
																END
														END ")"|| wait_type COLLATE Latin1_General_Bin2
												),
												NCHAR(31),N"?"),NCHAR(30),N"?"),NCHAR(29),N"?"),NCHAR(28),N"?"),NCHAR(27),N"?"),NCHAR(26),N"?"),NCHAR(25),N"?"),NCHAR(24),N"?"),NCHAR(23),N"?"),NCHAR(22),N"?"),
												NCHAR(21),N"?"),NCHAR(20),N"?"),NCHAR(19),N"?"),NCHAR(18),N"?"),NCHAR(17),N"?"),NCHAR(16),N"?"),NCHAR(15),N"?"),NCHAR(14),N"?"),NCHAR(12),N"?"),
												NCHAR(11),N"?"),NCHAR(8),N"?"),NCHAR(7),N"?"),NCHAR(6),N"?"),NCHAR(5),N"?"),NCHAR(4),N"?"),NCHAR(3),N"?"),NCHAR(2),N"?"),NCHAR(1),N"?"),
											NCHAR(0),
											N""
										) AS [waits]
									FROM
									(
										SELECT TOP(V_i)
											w1.*,
											ROW_NUMBER() OVER
											(
												PARTITION BY
													w1.session_id,
													w1.request_id
												ORDER BY
													w1.block_info DESC,
													w1.num_waits DESC,
													w1.wait_type
											) AS r
										FROM
										(
											SELECT TOP(V_i)
												task_info.session_id,
												task_info.request_id,
												task_info.physical_io,
												task_info.context_switches,
												task_info.thread_CPU_snapshot,
												task_info.num_tasks AS tasks,
												CASE
													WHEN task_info.runnable_time IS NOT NULL THEN
														"RUNNABLE"
													ELSE
														wt2.wait_type
												END AS wait_type,
												NULLIF(COUNT(COALESCE(task_info.runnable_time, wt2.waiting_task_address)), 0) AS num_waits,
												MIN(COALESCE(task_info.runnable_time, wt2.wait_duration_ms)) AS min_wait_time,
												AVG(COALESCE(task_info.runnable_time, wt2.wait_duration_ms)) AS avg_wait_time,
												MAX(COALESCE(task_info.runnable_time, wt2.wait_duration_ms)) AS max_wait_time,
												MAX(wt2.block_info) AS block_info
											FROM
											(
												SELECT TOP(V_i)
													t.session_id,
													t.request_id,
													SUM(CONVERT(BIGINT, t.pending_io_count)) OVER (PARTITION BY t.session_id, t.request_id) AS physical_io,
													SUM(CONVERT(BIGINT, t.context_switches_count)) OVER (PARTITION BY t.session_id, t.request_id) AS context_switches, 
													' +
													CASE
														WHEN 
															V_output_column_list LIKE '%|[CPU_delta|]%' ESCAPE '|'
															AND V_sys_info = 1
															THEN
																'SUM(tr.usermode_time + tr.kernel_time) OVER (PARTITION BY t.session_id, t.request_id) '
														ELSE
															'CONVERT(BIGINT, NULL) '
													END + 
														' AS thread_CPU_snapshot, 
													COUNT(*) OVER (PARTITION BY t.session_id, t.request_id) AS num_tasks,
													t.task_address,
													t.task_state,
													CASE
														WHEN
															t.task_state = "RUNNABLE"
															AND w.runnable_time > 0 THEN
																w.runnable_time
														ELSE
															NULL
													END AS runnable_time
												FROM sys.dm_os_tasks AS t
												CROSS APPLY
												(
													SELECT TOP(1)
														sp2.session_id
													FROM V_sessions AS sp2
													WHERE
														sp2.session_id = t.session_id
														AND sp2.request_id = t.request_id
														AND sp2.status <> "sleeping"
												) AS sp20
												LEFT OUTER HASH JOIN
												( 
												' +
													CASE
														WHEN V_sys_info = 1 THEN
															'SELECT TOP(V_i)
																(
																	SELECT TOP(V_i)
																		ms_ticks
																	FROM sys.dm_os_sys_info
																) -
																	w0.wait_resumed_ms_ticks AS runnable_time,
																w0.worker_address,
																w0.thread_address,
																w0.task_bound_ms_ticks
															FROM sys.dm_os_workers AS w0
															WHERE
																w0.state = "RUNNABLE"
																OR V_first_collection_ms_ticks >= w0.task_bound_ms_ticks'
														ELSE
															'SELECT
																CONVERT(BIGINT, NULL) AS runnable_time,
																CONVERT(VARBINARY(8), NULL) AS worker_address,
																CONVERT(VARBINARY(8), NULL) AS thread_address,
																CONVERT(BIGINT, NULL) AS task_bound_ms_ticks
															WHERE
																1 = 0'
														END +
												'
												) AS w ON
													w.worker_address = t.worker_address 
												' +
												CASE
													WHEN
														V_output_column_list LIKE '%|[CPU_delta|]%' ESCAPE '|'
														AND V_sys_info = 1
														THEN
															'LEFT OUTER HASH JOIN sys.dm_os_threads AS tr ON
																tr.thread_address = w.thread_address
																AND V_first_collection_ms_ticks >= w.task_bound_ms_ticks
															'
													ELSE
														''
												END +
											') AS task_info
											LEFT OUTER HASH JOIN
											(
												SELECT TOP(V_i)
													wt1.wait_type,
													wt1.waiting_task_address,
													MAX(wt1.wait_duration_ms) AS wait_duration_ms,
													MAX(wt1.block_info) AS block_info
												FROM
												(
													SELECT DISTINCT TOP(V_i)
														wt.wait_type +
															CASE
																WHEN wt.wait_type LIKE N"PAGE%LATCH_%" THEN
																	":"||
																	COALESCE(DB_NAME(CONVERT(INT, LEFT(wt.resource_description, CHARINDEX(N":", wt.resource_description) - 1))), N"(null)") ":"||
																	SUBSTRING(wt.resource_description, CHARINDEX(N":", wt.resource_description) + 1, LEN(wt.resource_description) - CHARINDEX(N":", REVERSE(wt.resource_description)) - CHARINDEX(N":", wt.resource_description)) "("||
																		CASE
																			WHEN
																				CONVERT(INT, RIGHT(wt.resource_description, CHARINDEX(N":", REVERSE(wt.resource_description)) - 1)) = 1 OR
																				CONVERT(INT, RIGHT(wt.resource_description, CHARINDEX(N":", REVERSE(wt.resource_description)) - 1)) % 8088 = 0
																					THEN 
																						N"PFS"
																			WHEN
																				CONVERT(INT, RIGHT(wt.resource_description, CHARINDEX(N":", REVERSE(wt.resource_description)) - 1)) = 2 OR
																				CONVERT(INT, RIGHT(wt.resource_description, CHARINDEX(N":", REVERSE(wt.resource_description)) - 1)) % 511232 = 0 
																					THEN 
																						N"GAM"
																			WHEN
																				CONVERT(INT, RIGHT(wt.resource_description, CHARINDEX(N":", REVERSE(wt.resource_description)) - 1)) = 3 OR
																				(CONVERT(INT, RIGHT(wt.resource_description, CHARINDEX(N":", REVERSE(wt.resource_description)) - 1)) - 1) % 511232 = 0 
																					THEN 
																						N"SGAM"
																			WHEN
																				CONVERT(INT, RIGHT(wt.resource_description, CHARINDEX(N":", REVERSE(wt.resource_description)) - 1)) = 6 OR
																				(CONVERT(INT, RIGHT(wt.resource_description, CHARINDEX(N":", REVERSE(wt.resource_description)) - 1)) - 6) % 511232 = 0 
																					THEN 
																						N"DCM"
																			WHEN
																				CONVERT(INT, RIGHT(wt.resource_description, CHARINDEX(N":", REVERSE(wt.resource_description)) - 1)) = 7 OR
																				(CONVERT(INT, RIGHT(wt.resource_description, CHARINDEX(N":", REVERSE(wt.resource_description)) - 1)) - 7) % 511232 = 0
																					THEN 
																						N"BCM"
																			ELSE
																				N"*"
																		END ")"
																WHEN wt.wait_type "CXPACKET" THEN
																	N":"|| SUBSTRING(wt.resource_description, CHARINDEX(N"nodeId", wt.resource_description) + 7, 4)
																WHEN wt.wait_type LIKE N"LATCH[_]%" THEN
																	N" ["|| LEFT(wt.resource_description, COALESCE(NULLIF(CHARINDEX(N" ", wt.resource_description), 0), LEN(wt.resource_description) + 1) - 1) "]"
																ELSE 
																	N""
															END COLLATE Latin1_General_Bin2 AS wait_type,
														CASE
															WHEN
															(
																wt.blocking_session_id IS NOT NULL
																AND wt.wait_type LIKE N"LCK[_]%"
															) THEN
																(
																	SELECT TOP(V_i)
																		x.lock_type,
																		REPLACE
																		(
																			REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
																			REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
																			REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
																				DB_NAME
																				(
																					CONVERT
																					(
																						INT,
																						SUBSTRING(wt.resource_description, NULLIF(CHARINDEX(N"dbid=", wt.resource_description), 0) + 5, COALESCE(NULLIF(CHARINDEX(N" ", wt.resource_description, CHARINDEX(N"dbid=", wt.resource_description) + 5), 0), LEN(wt.resource_description) + 1) - CHARINDEX(N"dbid=", wt.resource_description) - 5)
																					)
																				),
																				NCHAR(31),N"?"),NCHAR(30),N"?"),NCHAR(29),N"?"),NCHAR(28),N"?"),NCHAR(27),N"?"),NCHAR(26),N"?"),NCHAR(25),N"?"),NCHAR(24),N"?"),NCHAR(23),N"?"),NCHAR(22),N"?"),
																				NCHAR(21),N"?"),NCHAR(20),N"?"),NCHAR(19),N"?"),NCHAR(18),N"?"),NCHAR(17),N"?"),NCHAR(16),N"?"),NCHAR(15),N"?"),NCHAR(14),N"?"),NCHAR(12),N"?"),
																				NCHAR(11),N"?"),NCHAR(8),N"?"),NCHAR(7),N"?"),NCHAR(6),N"?"),NCHAR(5),N"?"),NCHAR(4),N"?"),NCHAR(3),N"?"),NCHAR(2),N"?"),NCHAR(1),N"?"),
																			NCHAR(0),
																			N""
																		) AS database_name,
																		CASE x.lock_type
																			WHEN N"objectlock" THEN
																				SUBSTRING(wt.resource_description, NULLIF(CHARINDEX(N"objid=", wt.resource_description), 0) + 6, COALESCE(NULLIF(CHARINDEX(N" ", wt.resource_description, CHARINDEX(N"objid=", wt.resource_description) + 6), 0), LEN(wt.resource_description) + 1) - CHARINDEX(N"objid=", wt.resource_description) - 6)
																			ELSE
																				NULL
																		END AS object_id,
																		CASE x.lock_type
																			WHEN N"filelock" THEN
																				SUBSTRING(wt.resource_description, NULLIF(CHARINDEX(N"fileid=", wt.resource_description), 0) + 7, COALESCE(NULLIF(CHARINDEX(N" ", wt.resource_description, CHARINDEX(N"fileid=", wt.resource_description) + 7), 0), LEN(wt.resource_description) + 1) - CHARINDEX(N"fileid=", wt.resource_description) - 7)
																			ELSE
																				NULL
																		END AS file_id,
																		CASE
																			WHEN x.lock_type in (N"pagelock", N"extentlock", N"ridlock") THEN
																				SUBSTRING(wt.resource_description, NULLIF(CHARINDEX(N"associatedObjectId=", wt.resource_description), 0) + 19, COALESCE(NULLIF(CHARINDEX(N" ", wt.resource_description, CHARINDEX(N"associatedObjectId=", wt.resource_description) + 19), 0), LEN(wt.resource_description) + 1) - CHARINDEX(N"associatedObjectId=", wt.resource_description) - 19)
																			WHEN x.lock_type in (N"keylock", N"hobtlock", N"allocunitlock") THEN
																				SUBSTRING(wt.resource_description, NULLIF(CHARINDEX(N"hobtid=", wt.resource_description), 0) + 7, COALESCE(NULLIF(CHARINDEX(N" ", wt.resource_description, CHARINDEX(N"hobtid=", wt.resource_description) + 7), 0), LEN(wt.resource_description) + 1) - CHARINDEX(N"hobtid=", wt.resource_description) - 7)
																			ELSE
																				NULL
																		END AS hobt_id,
																		CASE x.lock_type
																			WHEN N"applicationlock" THEN
																				SUBSTRING(wt.resource_description, NULLIF(CHARINDEX(N"hash=", wt.resource_description), 0) + 5, COALESCE(NULLIF(CHARINDEX(N" ", wt.resource_description, CHARINDEX(N"hash=", wt.resource_description) + 5), 0), LEN(wt.resource_description) + 1) - CHARINDEX(N"hash=", wt.resource_description) - 5)
																			ELSE
																				NULL
																		END AS applock_hash,
																		CASE x.lock_type
																			WHEN N"metadatalock" THEN
																				SUBSTRING(wt.resource_description, NULLIF(CHARINDEX(N"subresource=", wt.resource_description), 0) + 12, COALESCE(NULLIF(CHARINDEX(N" ", wt.resource_description, CHARINDEX(N"subresource=", wt.resource_description) + 12), 0), LEN(wt.resource_description) + 1) - CHARINDEX(N"subresource=", wt.resource_description) - 12)
																			ELSE
																				NULL
																		END AS metadata_resource,
																		CASE x.lock_type
																			WHEN N"metadatalock" THEN
																				SUBSTRING(wt.resource_description, NULLIF(CHARINDEX(N"classid=", wt.resource_description), 0) + 8, COALESCE(NULLIF(CHARINDEX(N" dbid=", wt.resource_description) - CHARINDEX(N"classid=", wt.resource_description), 0), LEN(wt.resource_description) + 1) - 8)
																			ELSE
																				NULL
																		END AS metadata_class_id
																	FROM
																	(
																		SELECT TOP(1)
																			LEFT(wt.resource_description, CHARINDEX(N" ", wt.resource_description) - 1) COLLATE Latin1_General_Bin2 AS lock_type
																	) AS x
																	FOR XML
																		PATH("")
																)
															ELSE NULL
														END AS block_info,
														wt.wait_duration_ms,
														wt.waiting_task_address
													FROM
													(
														SELECT TOP(V_i)
															wt0.wait_type COLLATE Latin1_General_Bin2 AS wait_type,
															wt0.resource_description COLLATE Latin1_General_Bin2 AS resource_description,
															wt0.wait_duration_ms,
															wt0.waiting_task_address,
															CASE
																WHEN wt0.blocking_session_id = p.blocked THEN
																	wt0.blocking_session_id
																ELSE
																	NULL
															END AS blocking_session_id
														FROM sys.dm_os_waiting_tasks AS wt0
														CROSS APPLY
														(
															SELECT TOP(1)
																s0.blocked
															FROM V_sessions AS s0
															WHERE
																s0.session_id = wt0.session_id
																AND COALESCE(s0.wait_type, N"") <> N"OLEDB"
																AND wt0.wait_type <> N"OLEDB"
														) AS p
													) AS wt
												) AS wt1
												GROUP BY
													wt1.wait_type,
													wt1.waiting_task_address
											) AS wt2 ON
												wt2.waiting_task_address = task_info.task_address
												AND wt2.wait_duration_ms > 0
												AND task_info.runnable_time IS NULL
											GROUP BY
												task_info.session_id,
												task_info.request_id,
												task_info.physical_io,
												task_info.context_switches,
												task_info.thread_CPU_snapshot,
												task_info.num_tasks,
												CASE
													WHEN task_info.runnable_time IS NOT NULL THEN
														"RUNNABLE"
													ELSE
														wt2.wait_type
												END
										) AS w1
									) AS waits
									ORDER BY
										waits.session_id,
										waits.request_id,
										waits.r
									FOR XML
										PATH(N"tasks"),
										TYPE
								) AS tasks_raw (task_xml_raw)
							) AS tasks_final
							CROSS APPLY tasks_final.task_xml.nodes("/tasks") AS task_nodes (task_node)
							WHERE
								task_nodes.task_node.exist("session_id") = 1
						) AS tasks ON
							tasks.session_id = y.session_id
							AND tasks.request_id = y.request_id 
						'
					ELSE
						''
				END ||
				'LEFT OUTER JOIN
				(
					SELECT TOP(V_i)
						t_info.session_id,
						COALESCE(t_info.request_id, -1) AS request_id,
						SUM(t_info.tempdb_allocations) AS tempdb_allocations,
						SUM(t_info.tempdb_current) AS tempdb_current
					FROM
					(
						SELECT TOP(V_i)
							tsu.session_id,
							tsu.request_id,
							tsu.user_objects_alloc_page_count +
								tsu.internal_objects_alloc_page_count AS tempdb_allocations,
							tsu.user_objects_alloc_page_count +
								tsu.internal_objects_alloc_page_count -
								tsu.user_objects_dealloc_page_count -
								tsu.internal_objects_dealloc_page_count AS tempdb_current
						FROM sys.dm_db_task_space_usage AS tsu
						CROSS APPLY
						(
							SELECT TOP(1)
								s0.session_id
							FROM V_sessions AS s0
							WHERE
								s0.session_id = tsu.session_id
						) AS p

						UNION ALL

						SELECT TOP(V_i)
							ssu.session_id,
							NULL AS request_id,
							ssu.user_objects_alloc_page_count +
								ssu.internal_objects_alloc_page_count AS tempdb_allocations,
							ssu.user_objects_alloc_page_count +
								ssu.internal_objects_alloc_page_count -
								ssu.user_objects_dealloc_page_count -
								ssu.internal_objects_dealloc_page_count AS tempdb_current
						FROM sys.dm_db_session_space_usage AS ssu
						CROSS APPLY
						(
							SELECT TOP(1)
								s0.session_id
							FROM V_sessions AS s0
							WHERE
								s0.session_id = ssu.session_id
						) AS p
					) AS t_info
					GROUP BY
						t_info.session_id,
						COALESCE(t_info.request_id, -1)
				) AS tempdb_info ON
					tempdb_info.session_id = y.session_id
					AND tempdb_info.request_id =
						CASE
							WHEN y.status "sleeping" THEN
								-1
							ELSE
								y.request_id
						END
				' ||
				CASE 
					WHEN 
						NOT 
						(
							V_get_avg_time = 1 
							AND V_recursion = 1
						) THEN 
							''
					ELSE
						'LEFT OUTER HASH JOIN
						(
							SELECT TOP(V_i)
								*
							FROM sys.dm_exec_query_stats
						) AS qs ON
							qs.sql_handle = y.sql_handle
							AND qs.plan_handle = y.plan_handle
							AND qs.statement_start_offset = y.statement_start_offset
							AND qs.statement_end_offset = y.statement_end_offset
						'
				END || 
			') AS x
			; ';
SET V_sql_n = CAST(V_sql AS STRING);
SET V_last_collection_start = current_timestamp();
IF 
			V_recursion = -1
			AND V_sys_info = 1
		THEN

SET V_first_collection_ms_ticks = (
SELECT
ms_ticks FROM sys.dm_os_sys_info LIMIT 1);
END IF;
		INSERT TEMP_TABLE_sessions
		(
			recursion,
			session_id,
			request_id,
			session_number,
			elapsed_time,
			avg_elapsed_time,
			physical_io,
			reads,
			physical_reads,
			writes,
			tempdb_allocations,
			tempdb_current,
			CPU,
			thread_CPU_snapshot,
			context_switches,
			used_memory,
			tasks,
			status,
			wait_info,
			transaction_id,
			open_tran_count,
			sql_handle,
			statement_start_offset,
			statement_end_offset,		
			sql_text,
			plan_handle,
			blocking_session_id,
			percent_complete,
			host_name,
			login_name,
			database_name,
			program_name,
			additional_info,
			start_time,
			login_time,
			last_request_start_time
		)
		;
CALL 
			V_sql_n,
			'V_recursion SMALLINT, V_filter string, V_not_filter string, V_first_collection_ms_ticks BIGINT',
			V_recursion, V_filter, V_not_filter, V_first_collection_ms_ticks;
--Collect transaction information?

IF
			V_recursion = 1
			AND
			(
				V_output_column_list LIKE '%|`tran_start_time|`%' ESCAPE '|'
				OR V_output_column_list LIKE '%|`tran_log_writes|`%' ESCAPE '|' 
			)
		THEN
SET V_i = 2147483647;
			MERGE INTO TEMP_TABLE_sessions s_TGT
USING (
WITH
AS as ( SELECT TOP ( V_i ) trans_nodes.trans_node.value ( '(session_id/STRING())`1`' , 'SMALLINT' ) AS session_id , COALESCE ( trans_nodes.trans_node.value ( '(request_id/STRING())`1`' , 'INT' ) , 0 ) AS request_id , trans_nodes.trans_node.value ( '(trans_info/STRING())`1`' , 'STRING' ) AS trans_info FROM ( SELECT TOP ( V_i ) CAST(REPLACE ( CAST(trans_raw.trans_xml_raw AS STRING) COLLATE Latin1_General_Bin2 , '</trans_info></trans><trans><trans_info>' , '' ) AS XML) FROM ( SELECT TOP ( V_i ) CASE u_trans.r WHEN 1 THEN u_trans.session_id ELSE NULL END AS `session_id` , CASE u_trans.r WHEN 1 THEN u_trans.request_id ELSE NULL END AS `request_id` , CAST(CASE WHEN u_trans.database_id IS NOT NULL THEN CASE u_trans.r WHEN 1 THEN COALESCE ( date_format(u_trans.transaction_start_time, 'yyyy-MM-dd hh:mm:ss:SSS + string ( 254 ) , 'INT'7 ) ELSE 'STRING'95 END + REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( CAST(COALESCE ( current_database() , 'STRING'96 ) AS STRING) , string ( 31 ) , 'STRING'97 ) , string ( 30 ) , 'STRING'98 ) , string ( 29 ) , 'STRING'99 ) , string ( 28 ) , '</trans_info></trans><trans><trans_info>'00 ) , string ( 27 ) , '</trans_info></trans><trans><trans_info>'01 ) , string ( 26 ) , '</trans_info></trans><trans><trans_info>'02 ) , string ( 25 ) , '</trans_info></trans><trans><trans_info>'03 ) , string ( 24 ) , '</trans_info></trans><trans><trans_info>'04 ) , string ( 23 ) , '</trans_info></trans><trans><trans_info>'05 ) , string ( 22 ) , '</trans_info></trans><trans><trans_info>'06 ) , string ( 21 ) , '</trans_info></trans><trans><trans_info>'07 ) , string ( 20 ) , '</trans_info></trans><trans><trans_info>'08 ) , string ( 19 ) , '</trans_info></trans><trans><trans_info>'09 ) , string ( 18 ) , '</trans_info></trans><trans><trans_info>'10 ) , string ( 17 ) , '</trans_info></trans><trans><trans_info>'11 ) , string ( 16 ) , '</trans_info></trans><trans><trans_info>'12 ) , string ( 15 ) , '</trans_info></trans><trans><trans_info>'13 ) , string ( 14 ) , '</trans_info></trans><trans><trans_info>'14 ) , string ( 12 ) , '</trans_info></trans><trans><trans_info>'15 ) , string ( 11 ) , '</trans_info></trans><trans><trans_info>'16 ) , string ( 8 ) , '</trans_info></trans><trans><trans_info>'17 ) , string ( 7 ) , '</trans_info></trans><trans><trans_info>'18 ) , string ( 6 ) , '</trans_info></trans><trans><trans_info>'19 ) , string ( 5 ) , '</trans_info></trans><trans><trans_info>'20 ) , string ( 4 ) , '</trans_info></trans><trans><trans_info>'21 ) , string ( 3 ) , '</trans_info></trans><trans><trans_info>'22 ) , string ( 2 ) , '</trans_info></trans><trans><trans_info>'23 ) , string ( 1 ) , '</trans_info></trans><trans><trans_info>'24 ) , string ( 0 ) , '</trans_info></trans><trans><trans_info>'25 ) || '</trans_info></trans><trans><trans_info>'26 + CAST(u_trans.log_record_count AS STRING) || '</trans_info></trans><trans><trans_info>'27 + CAST(u_trans.log_kb_used AS STRING) || '</trans_info></trans><trans><trans_info>'28 '</trans_info></trans><trans><trans_info>'29 ELSE '</trans_info></trans><trans><trans_info>'30 END COLLATE Latin1_General_Bin2 AS STRING) AS `trans_info` FROM ( SELECT TOP ( V_i ) trans. * , ROW_NUMBER ( ) OVER ( PARTITION BY trans.session_id , trans.request_id ORDER BY trans.transaction_start_time DESC ) AS r FROM ( SELECT TOP ( V_i ) session_tran_map.session_id , session_tran_map.request_id , s_tran.database_id , COALESCE ( SUM ( s_tran.database_transaction_log_record_count ) , 0 ) AS log_record_count , COALESCE ( SUM ( s_tran.database_transaction_log_bytes_used ) , 0 ) / 1024 AS log_kb_used , MIN ( s_tran.database_transaction_begin_time ) AS transaction_start_time FROM ( SELECT TOP ( V_i ) * FROM sys.dm_tran_active_transactions WHERE transaction_begin_time <= V_last_collection_start ) AS a_tran INNER JOIN ( SELECT TOP ( V_i ) * FROM sys.dm_tran_database_transactions WHERE database_id < 32767 ) s_tran ON s_tran.transaction_id = a_tran.transaction_id LEFT JOIN ( SELECT TOP ( V_i ) * FROM sys.dm_tran_session_transactions ) tst ON s_tran.transaction_id = tst.transaction_id CROSS APPLY ( SELECT TOP ( 1 ) s3.session_id , s3.request_id FROM ( SELECT TOP ( 1 ) s1.session_id , s1.request_id FROM TEMP_TABLE_sessions s1 WHERE s1.transaction_id = s_tran.transaction_id AND s1.recursion = 1 UNION ALL SELECT TOP ( 1 ) s2.session_id , s2.request_id FROM TEMP_TABLE_sessions s2 WHERE s2.session_id = tst.session_id AND s2.recursion = 1 ) AS s3 ORDER BY s3.request_id ) AS session_tran_map GROUP BY session_tran_map.session_id , session_tran_map.request_id , s_tran.database_id ) AS trans ) AS u_trans FOR XML PATH ( 'trans' ) , TYPE ) AS trans_raw ( trans_xml_raw ) ) AS trans_final ( trans_xml ) CROSS APPLY trans_final.trans_xml.nodes ( '/trans' ) AS trans_nodes ( trans_node ) )
SELECT * 
FROM AS x
INNER JOIN TEMP_TABLE_sessions s ON s.session_id = x.session_id AND s.request_id = x.request_id 
)
ON 
COALESCE(s.session_id::string,'__NULL__') = COALESCE(s_TGT.session_id::string,'__NULL__') AND 
COALESCE(s.request_id::string,'__NULL__') = COALESCE(s_TGT.request_id::string,'__NULL__')
WHEN MATCHED THEN UPDATE SET
tran_start_time = date_format(LEFT ( x.trans_info , NULLIF ( INSTR(x.trans_info, CHAR(254) COLLATE Latin1_General_Bin2) - 1 , - 1 ) ), 'yyyy-MM-dd hh:mm:ss:SSS ,
tran_log_writes = RIGHT ( x.trans_info , LEN ( x.trans_info ) - INSTR(x.trans_info, CHAR(254) COLLATE Latin1_General_Bin2) );
END IF;
--Variables for text and plan collection

IF 
			V_recursion = 1
			AND V_output_column_list LIKE '%|`sql_text|`%' ESCAPE '|'
		THEN
SELECT 
					session_id,
					request_id,
					sql_handle,
					statement_start_offset,
					statement_end_offset
				FROM TEMP_TABLE_sessions
				WHERE
					recursion = 1
					AND sql_handle IS NOT NULL
			;
			OPEN sql_cursor;
			FETCH NEXT FROM sql_cursor
			INTO 
				V_session_id,
				V_request_id,
				V_sql_handle,
				V_statement_start_offset,
				V_statement_end_offset;
			--Wait up to 5 ms for the SQL text, then give up
			SET LOCK_TIMEOUT 5;
WHILE V_V_FETCH_STATUS = 0
			THEN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
DO
GET DIAGNOSTICS CONDITION 1
;
					MERGE INTO TEMP_TABLE_sessions s_TGT
USING TEMP_TABLE_sessions s
ON s.session_id = V_session_id AND s.request_id = V_request_id AND s.recursion = 1  AND 
COALESCE(s.sql_text::string,'__NULL__') = COALESCE(s_TGT.sql_text::string,'__NULL__') AND 
COALESCE(s.session_id::string,'__NULL__') = COALESCE(s_TGT.session_id::string,'__NULL__') AND 
COALESCE(s.request_id::string,'__NULL__') = COALESCE(s_TGT.request_id::string,'__NULL__') AND 
COALESCE(s.recursion::string,'__NULL__') = COALESCE(s_TGT.recursion::string,'__NULL__')
WHEN MATCHED THEN UPDATE SET
sql_text = CASE LINE_NUMBER WHEN 1222 THEN '<timeout_exceeded />' ELSE '<error message="' + MESSAGE_TEXT + '" />' END;

END WHILE;
					MERGE INTO TEMP_TABLE_sessions s_TGT
USING TEMP_TABLE_sessions s , ( SELECT TOP ( 1 ) STRING FROM ( SELECT STRING , 0 AS row_num FROM sys.dm_exec_sql_text ( V_sql_handle ) UNION ALL SELECT NULL , 1 AS row_num ) AS est0 ORDER BY row_num ) AS est
ON s.session_id = V_session_id AND s.request_id = V_request_id AND s.recursion = 1  AND 
COALESCE(s.sql_text::string,'__NULL__') = COALESCE(s_TGT.sql_text::string,'__NULL__') AND 
COALESCE(s.statement_start_offset::string,'__NULL__') = COALESCE(s_TGT.statement_start_offset::string,'__NULL__') AND 
COALESCE(s.statement_end_offset::string,'__NULL__') = COALESCE(s_TGT.statement_end_offset::string,'__NULL__') AND 
COALESCE(s.session_id::string,'__NULL__') = COALESCE(s_TGT.session_id::string,'__NULL__') AND 
COALESCE(s.request_id::string,'__NULL__') = COALESCE(s_TGT.request_id::string,'__NULL__') AND 
COALESCE(s.recursion::string,'__NULL__') = COALESCE(s_TGT.recursion::string,'__NULL__')
WHEN MATCHED THEN UPDATE SET
sql_text = ( SELECT REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( '--' || CHAR(13) + CHAR(10) + CASE WHEN V_get_full_inner_text = 1 THEN est.STRING WHEN LEN ( est.STRING ) < ( V_statement_end_offset / 2 ) + 1 THEN est.STRING WHEN SUBSTRING ( est.STRING , ( V_statement_start_offset / 2 ) , 2 ) LIKE '?'36 THEN est.STRING ELSE CASE WHEN V_statement_start_offset > 0 THEN SUBSTRING ( est.STRING , ( ( V_statement_start_offset / 2 ) + 1 ) , ( CASE WHEN V_statement_end_offset = - 1 THEN 2147483647 ELSE ( ( V_statement_end_offset - V_statement_start_offset ) / 2 ) + 1 END ) ) ELSE RTRIM ( LTRIM ( est.STRING ) ) END END + CHAR(13) + CHAR(10) || '--' COLLATE Latin1_General_Bin2 , CHAR(31) , '?' ) , CHAR(30) , '?' ) , CHAR(29) , '?' ) , CHAR(28) , '?' ) , CHAR(27) , '?' ) , CHAR(26) , '?' ) , CHAR(25) , '?' ) , CHAR(24) , '?' ) , CHAR(23) , '?' ) , CHAR(22) , '?' ) , CHAR(21) , '?' ) , CHAR(20) , '?' ) , CHAR(19) , '?' ) , CHAR(18) , '?' ) , CHAR(17) , '?' ) , CHAR(16) , '?' ) , CHAR(15) , '?' ) , CHAR(14) , '?' ) , CHAR(12) , '?' ) , CHAR(11) , '?' ) , CHAR(8) , '?' ) , CHAR(7) , '?' ) , CHAR(6) , '?' ) , CHAR(5) , '?' ) , CHAR(4) , '?' ) , CHAR(3) , '?' ) , CHAR(2) , '?' ) , CHAR(1) , '?' ) , CHAR(0) , '' ) AS `processing - instruction_query_` FOR XML PATH ( '' ) , TYPE ) ,
statement_start_offset = CASE WHEN LEN ( est.STRING ) < ( V_statement_end_offset / 2 ) + 1 THEN 0 WHEN SUBSTRING ( CAST(est.STRING AS STRING) , ( V_statement_start_offset / 2 ) , 2 ) LIKE '?'68 THEN 0 ELSE V_statement_start_offset END ,
statement_end_offset = CASE WHEN LEN ( est.STRING ) < ( V_statement_end_offset / 2 ) + 1 THEN - 1 WHEN SUBSTRING ( CAST(est.STRING AS STRING) , ( V_statement_start_offset / 2 ) , 2 ) LIKE '?'69 THEN - 1 ELSE V_statement_end_offset END;

				FETCH NEXT FROM sql_cursor
				INTO
					V_session_id,
					V_request_id,
					V_sql_handle,
					V_statement_start_offset,
					V_statement_end_offset;
END IF;
			--Return this to the default
			SET LOCK_TIMEOUT -1;
			CLOSE sql_cursor;
			DEALLOCATE sql_cursor;
END IF;
IF 
			V_get_outer_command = 1 
			AND V_recursion = 1
			AND V_output_column_list LIKE '%|`sql_command|`%' ESCAPE '|'
		THEN
CREATE TEMPORARY TABLE V_buffer_results
(
EventType STRING,
Parameters INT,
EventInfo STRING,
start_time TIMESTAMP,
session_number BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL PRIMARY KEY
);
SELECT 
					session_id,
					MAX(start_time) AS start_time
				FROM TEMP_TABLE_sessions
				WHERE
					recursion = 1
				GROUP BY
					session_id
				ORDER BY
					session_id
				;
			OPEN buffer_cursor;
			FETCH NEXT FROM buffer_cursor
			INTO 
				V_session_id,
				V_start_time;
WHILE V_V_FETCH_STATUS = 0
			THEN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
DO
GET DIAGNOSTICS CONDITION 1

				;

END WHILE;
--In SQL Server 2008, DBCC INPUTBUFFER will throw 
					--an exception if the session no longer exists

					INSERT V_buffer_results
					(
						EventType,
						Parameters,
						EventInfo
					)
					;
CALL
						'DBCC INPUTBUFFER(V_session_id) WITH NO_INFOMSGS;',
						'V_session_id SMALLINT',
						V_session_id;
					MERGE INTO V_buffer_results br_TGT
USING V_buffer_results br
ON br.session_number = ( SELECT MAX ( br2.session_number ) FROM V_buffer_results br2 ) AND 
COALESCE(br.start_time::string,'__NULL__') = COALESCE(br_TGT.start_time::string,'__NULL__') AND 
COALESCE(br.session_number::string,'__NULL__') = COALESCE(br_TGT.session_number::string,'__NULL__')
WHEN MATCHED THEN UPDATE SET
start_time = V_start_time;

				FETCH NEXT FROM buffer_cursor
				INTO 
					V_session_id,
					V_start_time;
END IF;
			MERGE INTO sys.dm_exec_sessions s_TGT
USING TEMP_TABLE_sessions s
ON recursion = 1  AND 
COALESCE(s.session_number::string,'__NULL__') = COALESCE(s_TGT.session_number::string,'__NULL__') AND 
COALESCE(s.start_time::string,'__NULL__') = COALESCE(s_TGT.start_time::string,'__NULL__') AND 
COALESCE(s.last_request_start_time::string,'__NULL__') = COALESCE(s_TGT.last_request_start_time::string,'__NULL__') AND 
COALESCE(s.session_id::string,'__NULL__') = COALESCE(s_TGT.session_id::string,'__NULL__') AND 
COALESCE(s.request_id::string,'__NULL__') = COALESCE(s_TGT.request_id::string,'__NULL__')
WHEN MATCHED THEN UPDATE SET
sql_command = ( SELECT REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( CAST('--' || CHAR(13) + CHAR(10) + br.EventInfo + CHAR(13) + CHAR(10) || '--' COLLATE Latin1_General_Bin2 AS STRING) , CHAR(31) , '?' ) , CHAR(30) , '?' ) , CHAR(29) , '?' ) , CHAR(28) , '?' ) , CHAR(27) , '?' ) , CHAR(26) , '?' ) , CHAR(25) , '?' ) , CHAR(24) , '?' ) , CHAR(23) , '?' ) , CHAR(22) , '?' ) , CHAR(21) , '?' ) , CHAR(20) , '?' ) , CHAR(19) , '?' ) , CHAR(18) , '?' ) , CHAR(17) , '?' ) , CHAR(16) , '?' ) , CHAR(15) , '?' ) , CHAR(14) , '?' ) , CHAR(12) , '?' ) , CHAR(11) , '?' ) , CHAR(8) , '?' ) , CHAR(7) , '?' ) , CHAR(6) , '?' ) , CHAR(5) , '?' ) , CHAR(4) , '?' ) , CHAR(3) , '?' ) , CHAR(2) , '?' ) , CHAR(1) , '?' ) , CHAR(0) , '' ) AS `processing - instruction_query_` FROM V_buffer_results br WHERE br.session_number = s.session_number AND br.start_time = s.start_time AND ( ( s.start_time = s.last_request_start_time AND EXISTS ( SELECT * FROM sys.dm_exec_requests r2 WHERE r2.session_id = s.session_id AND r2.request_id = s.request_id AND r2.start_time = s.start_time ) ) OR ( s.request_id = 0 AND EXISTS ( SELECT * FROM sys.dm_exec_sessions s2 WHERE s2.session_id = s.session_id AND s2.last_request_start_time = s.last_request_start_time ) ) ) FOR XML PATH ( '' ) , TYPE );
			CLOSE buffer_cursor;
			DEALLOCATE buffer_cursor;
END IF;
IF 
			V_get_plans >= 1 
			AND V_recursion = 1
			AND V_output_column_list LIKE '%|`query_plan|`%' ESCAPE '|'
		THEN
SET V_live_plan = COALESCE(CAST(SIGN(OBJECT_ID('sys.dm_exec_query_statistics_xml')) AS BOOLEAN), 0)

			;
SELECT
					session_id,
					request_id,
					plan_handle,
					statement_start_offset,
					statement_end_offset
				FROM TEMP_TABLE_sessions
				WHERE
					recursion = 1
					AND plan_handle IS NOT NULL
			;
			OPEN plan_cursor;
			FETCH NEXT FROM plan_cursor
			INTO 
				V_session_id,
				V_request_id,
				V_plan_handle,
				V_statement_start_offset,
				V_statement_end_offset;
			--Wait up to 5 ms for a query plan, then give up
			SET LOCK_TIMEOUT 5;
WHILE V_V_FETCH_STATUS = 0
			THEN
SET V_query_plan = NULL;
IF V_live_plan = 1
				THEN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
DO
GET DIAGNOSTICS CONDITION 1
;
SET V_query_plan = NULL;

END WHILE;

SET V_query_plan = (
SELECT
x.query_plan FROM sys.dm_exec_query_statistics_xml(V_session_id) AS x LIMIT 1);
IF 
							V_query_plan IS NOT NULL
							AND EXISTS
							(
								SELECT
									*
								FROM sys.dm_exec_requests AS r
								WHERE
									r.session_id = V_session_id
									AND r.request_id = V_request_id
									AND r.plan_handle = V_plan_handle
									AND r.statement_start_offset = V_statement_start_offset
									AND r.statement_end_offset = V_statement_end_offset
							)
						THEN
							MERGE INTO TEMP_TABLE_sessions s_TGT
USING TEMP_TABLE_sessions s
ON s.session_id = V_session_id AND s.request_id = V_request_id AND s.recursion = 1  AND 
COALESCE(s.query_plan::string,'__NULL__') = COALESCE(s_TGT.query_plan::string,'__NULL__') AND 
COALESCE(s.session_id::string,'__NULL__') = COALESCE(s_TGT.session_id::string,'__NULL__') AND 
COALESCE(s.request_id::string,'__NULL__') = COALESCE(s_TGT.request_id::string,'__NULL__') AND 
COALESCE(s.recursion::string,'__NULL__') = COALESCE(s_TGT.recursion::string,'__NULL__')
WHEN MATCHED THEN UPDATE SET
query_plan = V_query_plan;

END IF;
IF V_query_plan IS NULL
				THEN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
DO
GET DIAGNOSTICS CONDITION 1
;
IF LINE_NUMBER = 6335
						THEN
							MERGE INTO TEMP_TABLE_sessions s_TGT
USING TEMP_TABLE_sessions s
ON s.session_id = V_session_id AND s.request_id = V_request_id AND s.recursion = 1  AND 
COALESCE(s.query_plan::string,'__NULL__') = COALESCE(s_TGT.query_plan::string,'__NULL__') AND 
COALESCE(s.session_id::string,'__NULL__') = COALESCE(s_TGT.session_id::string,'__NULL__') AND 
COALESCE(s.request_id::string,'__NULL__') = COALESCE(s_TGT.request_id::string,'__NULL__') AND 
COALESCE(s.recursion::string,'__NULL__') = COALESCE(s_TGT.recursion::string,'__NULL__')
WHEN MATCHED THEN UPDATE SET
query_plan = ( SELECT '--' || CHAR(13) + CHAR(10) || '-- Could not render showplan due to XML data type limitations. ' || CHAR(13) + CHAR(10) || '-- To see the graphical plan save the XML below as a .SQLPLAN file and re-open in SSMS.' || CHAR(13) + CHAR(10) || '--' || CHAR(13) + CHAR(10) + REPLACE ( qp.query_plan , '<RelOp' , CHAR(13) + CHAR(10) || '<RelOp' ) + CHAR(13) + CHAR(10) || '--' COLLATE Latin1_General_Bin2 AS `processing - instruction_query_plan_` FROM sys.dm_exec_text_query_plan ( V_plan_handle , CASE V_get_plans WHEN 1 THEN V_statement_start_offset ELSE 0 END , CASE V_get_plans WHEN 1 THEN V_statement_end_offset ELSE - 1 END ) AS qp FOR XML PATH ( '' ) , TYPE );
ELSE

							MERGE INTO TEMP_TABLE_sessions s_TGT
USING TEMP_TABLE_sessions s
ON s.session_id = V_session_id AND s.request_id = V_request_id AND s.recursion = 1  AND 
COALESCE(s.query_plan::string,'__NULL__') = COALESCE(s_TGT.query_plan::string,'__NULL__') AND 
COALESCE(s.session_id::string,'__NULL__') = COALESCE(s_TGT.session_id::string,'__NULL__') AND 
COALESCE(s.request_id::string,'__NULL__') = COALESCE(s_TGT.request_id::string,'__NULL__') AND 
COALESCE(s.recursion::string,'__NULL__') = COALESCE(s_TGT.recursion::string,'__NULL__')
WHEN MATCHED THEN UPDATE SET
query_plan = CASE LINE_NUMBER WHEN 1222 THEN '<timeout_exceeded />' ELSE '<error message="' + MESSAGE_TEXT + '" />' END;

END WHILE;
						MERGE INTO TEMP_TABLE_sessions s_TGT
USING TEMP_TABLE_sessions s
ON s.session_id = V_session_id AND s.request_id = V_request_id AND s.recursion = 1  AND 
COALESCE(s.query_plan::string,'__NULL__') = COALESCE(s_TGT.query_plan::string,'__NULL__') AND 
COALESCE(s.session_id::string,'__NULL__') = COALESCE(s_TGT.session_id::string,'__NULL__') AND 
COALESCE(s.request_id::string,'__NULL__') = COALESCE(s_TGT.request_id::string,'__NULL__') AND 
COALESCE(s.recursion::string,'__NULL__') = COALESCE(s_TGT.recursion::string,'__NULL__')
WHEN MATCHED THEN UPDATE SET
query_plan = ( SELECT CAST(query_plan AS xml) FROM sys.dm_exec_text_query_plan ( V_plan_handle , CASE V_get_plans WHEN 1 THEN V_statement_start_offset ELSE 0 END , CASE V_get_plans WHEN 1 THEN V_statement_end_offset ELSE - 1 END ) );

END IF;
				FETCH NEXT FROM plan_cursor
				INTO
					V_session_id,
					V_request_id,
					V_plan_handle,
					V_statement_start_offset,
					V_statement_end_offset;
END IF;
			--Return this to the default
			SET LOCK_TIMEOUT -1;
			CLOSE plan_cursor;
			DEALLOCATE plan_cursor;
END IF;
IF 
			V_get_locks = 1 
			AND V_recursion = 1
			AND V_output_column_list LIKE '%|`locks|`%' ESCAPE '|'
		THEN
SELECT DISTINCT
					database_name
				FROM TEMP_TABLE_locks
				WHERE
					EXISTS
					(
						SELECT *
						FROM TEMP_TABLE_sessions AS s
						WHERE
							s.session_id = TEMP_TABLE_locks.session_id
							AND recursion = 1
					)
					AND database_name <> '(null)'
				;
			OPEN locks_cursor;
			FETCH NEXT FROM locks_cursor
			INTO 
				V_database_name;
WHILE V_V_FETCH_STATUS = 0
			THEN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
DO
GET DIAGNOSTICS CONDITION 1
;
					UPDATE TEMP_TABLE_locks
					SET
						query_error = 
							REPLACE
							(
								REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
								REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
								REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
									CAST(MESSAGE_TEXT COLLATE Latin1_General_Bin2 AS STRING),
									CHAR(31),'?'),CHAR(30),'?'),CHAR(29),'?'),CHAR(28),'?'),CHAR(27),'?'),CHAR(26),'?'),CHAR(25),'?'),CHAR(24),'?'),CHAR(23),'?'),CHAR(22),'?'),
									CHAR(21),'?'),CHAR(20),'?'),CHAR(19),'?'),CHAR(18),'?'),CHAR(17),'?'),CHAR(16),'?'),CHAR(15),'?'),CHAR(14),'?'),CHAR(12),'?'),
									CHAR(11),'?'),CHAR(8),'?'),CHAR(7),'?'),CHAR(6),'?'),CHAR(5),'?'),CHAR(4),'?'),CHAR(3),'?'),CHAR(2),'?'),CHAR(1),'?'),
								CHAR(0),
								''
							)
					WHERE 
						database_name = V_database_name
					;

END WHILE;
CALL
						V_sql_n,
						'V_database_name string',
						V_database_name;

				FETCH NEXT FROM locks_cursor
				INTO
					V_database_name;
END IF;
			CLOSE locks_cursor;
			DEALLOCATE locks_cursor;
			MERGE INTO TEMP_TABLE_sessions s_TGT
USING TEMP_TABLE_sessions s 
ON 
COALESCE(s.locks::string,'__NULL__') = COALESCE(s_TGT.locks::string,'__NULL__') AND 
COALESCE(s.session_id::string,'__NULL__') = COALESCE(s_TGT.session_id::string,'__NULL__') AND 
COALESCE(s.request_id::string,'__NULL__') = COALESCE(s_TGT.request_id::string,'__NULL__') AND 
COALESCE(s.start_time::string,'__NULL__') = COALESCE(s_TGT.start_time::string,'__NULL__') AND 
COALESCE(s.last_request_start_time::string,'__NULL__') = COALESCE(s_TGT.last_request_start_time::string,'__NULL__') AND 
COALESCE(s.recursion::string,'__NULL__') = COALESCE(s_TGT.recursion::string,'__NULL__')
WHEN MATCHED THEN UPDATE SET
locks = ( SELECT REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( CAST(l1.database_name COLLATE Latin1_General_Bin2 AS STRING) , CHAR(31) , '?' ) , CHAR(30) , '?' ) , CHAR(29) , '?' ) , CHAR(28) , '?' ) , CHAR(27) , '?' ) , CHAR(26) , '?' ) , CHAR(25) , '?' ) , CHAR(24) , '?' ) , CHAR(23) , '?' ) , CHAR(22) , '?' ) , CHAR(21) , '?' ) , CHAR(20) , '?' ) , CHAR(19) , '?' ) , CHAR(18) , '?' ) , CHAR(17) , '?' ) , CHAR(16) , '?' ) , CHAR(15) , '?' ) , CHAR(14) , '?' ) , CHAR(12) , '?' ) , CHAR(11) , '?' ) , CHAR(8) , '?' ) , CHAR(7) , '?' ) , CHAR(6) , '?' ) , CHAR(5) , '?' ) , CHAR(4) , '?' ) , CHAR(3) , '?' ) , CHAR(2) , '?' ) , CHAR(1) , '?' ) , CHAR(0) , '' ) AS `Database / V_name` , MIN ( l1.query_error ) AS `Database / V_query_error` , ( SELECT l2.request_mode AS `Lock / V_request_mode` , l2.request_status AS `Lock / V_request_status` , COUNT ( * ) AS `Lock / V_request_count` FROM TEMP_TABLE_locks l2 WHERE l1.session_id = l2.session_id AND l1.request_id = l2.request_id AND l2.database_name = l1.database_name AND l2.resource_type = 'DATABASE' GROUP BY l2.request_mode , l2.request_status FOR XML PATH ( '' ) , TYPE ) AS `Database / Locks` , ( SELECT COALESCE ( l3.object_name , '(null)' ) AS `Object / V_name` , l3.schema_name AS `Object / V_schema_name` , ( SELECT l4.resource_type AS `Lock / V_resource_type` , l4.page_type AS `Lock / V_page_type` , l4.index_name AS `Lock / V_index_name` , CASE WHEN l4.object_name IS NULL THEN l4.schema_name ELSE NULL END AS `Lock / V_schema_name` , l4.principal_name AS `Lock / V_principal_name` , l4.resource_description AS `Lock / V_resource_description` , l4.request_mode AS `Lock / V_request_mode` , l4.request_status AS `Lock / V_request_status` , SUM ( l4.request_count ) AS `Lock / V_request_count` FROM TEMP_TABLE_locks l4 WHERE l4.session_id = l3.session_id AND l4.request_id = l3.request_id AND l3.database_name = l4.database_name AND COALESCE ( l3.object_name , '(null)' ) = COALESCE ( l4.object_name , '(null)' ) AND COALESCE ( l3.schema_name , '' ) = COALESCE ( l4.schema_name , '' ) AND l4.resource_type <> 'DATABASE' GROUP BY l4.resource_type , l4.page_type , l4.index_name , CASE WHEN l4.object_name IS NULL THEN l4.schema_name ELSE NULL END , l4.principal_name , l4.resource_description , l4.request_mode , l4.request_status FOR XML PATH ( '' ) , TYPE ) AS `Object / Locks` FROM TEMP_TABLE_locks AS l3 WHERE l3.session_id = l1.session_id AND l3.request_id = l1.request_id AND l3.database_name = l1.database_name AND l3.resource_type <> 'DATABASE' GROUP BY l3.session_id , l3.request_id , l3.database_name , COALESCE ( l3.object_name , '(null)' ) , l3.schema_name FOR XML PATH ( '' ) , TYPE ) AS `Database / Objects` FROM TEMP_TABLE_locks AS l1 WHERE l1.session_id = s.session_id AND l1.request_id = s.request_id AND l1.start_time IN ( s.start_time , s.last_request_start_time ) AND s.recursion = 1 GROUP BY l1.session_id , l1.request_id , l1.database_name FOR XML PATH ( '' ) , TYPE );
END IF;
IF 
			V_find_block_leaders = 1
			AND V_recursion = 1
			AND V_output_column_list LIKE '%|`blocked_session_count|`%' ESCAPE '|'
		THEN
WITH
			blockers AS
			(
				SELECT
					session_id,
					session_id AS top_level_session_id,
					CAST('.' || CAST(session_id AS STRING) || '.' AS STRING) AS the_path
				FROM TEMP_TABLE_sessions
				WHERE
					recursion = 1

				UNION ALL

				SELECT
					s.session_id,
					b.top_level_session_id,
					CAST(b.the_path + CAST(s.session_id AS STRING) || '.' AS STRING) AS the_path
				FROM blockers AS b
				JOIN TEMP_TABLE_sessions AS s ON
					s.blocking_session_id = b.session_id
					AND s.recursion = 1
					AND b.the_path NOT LIKE '%.' || CAST(s.session_id AS STRING) || '.%' COLLATE Latin1_General_Bin2
			)
			MERGE INTO TEMP_TABLE_sessions s_TGT
USING (
WITH
x as ( SELECT b.top_level_session_id AS session_id , COUNT ( * ) - 1 AS blocked_session_count FROM blockers b GROUP BY b.top_level_session_id )
SELECT * 
FROM TEMP_TABLE_sessions s
INNER JOIN x ON s.session_id = x.session_id
)
ON s.recursion = 1 AND 
COALESCE(s.blocked_session_count::string,'__NULL__') = COALESCE(s_TGT.blocked_session_count::string,'__NULL__') AND 
COALESCE(s.session_id::string,'__NULL__') = COALESCE(s_TGT.session_id::string,'__NULL__') AND 
COALESCE(s.recursion::string,'__NULL__') = COALESCE(s_TGT.recursion::string,'__NULL__')
WHEN MATCHED THEN UPDATE SET
blocked_session_count = x.blocked_session_count;
END IF;
IF
			V_get_task_info = 2
			AND V_output_column_list LIKE '%|`additional_info|`%' ESCAPE '|'
			AND V_recursion = 1
		THEN
CREATE TEMPORARY TABLE TEMP_TABLE_blocked_requests
			(
				session_id SMALLINT NOT NULL,
				request_id INT NOT NULL,
				database_name string NOT NULL,
				object_id INT,
				hobt_id BIGINT,
				schema_id INT,
				schema_name string ,
				object_name string ,
				query_error STRING,
				PRIMARY KEY (database_name, session_id, request_id)
			);
WITH SAMPLE 0 ROWS, NORECOMPUTE;
WITH SAMPLE 0 ROWS, NORECOMPUTE;
WITH SAMPLE 0 ROWS, NORECOMPUTE;
WITH SAMPLE 0 ROWS, NORECOMPUTE;
			INSERT TEMP_TABLE_blocked_requests
			(
				session_id,
				request_id,
				database_name,
				object_id,
				hobt_id,
				schema_id
			)
			SELECT
				session_id,
				request_id,
				database_name,
				object_id,
				hobt_id,
				CAST(SUBSTRING(schema_node, INSTR(schema_node, ' = ') + 3, LEN(schema_node)) AS INT) AS schema_id
			FROM
			(
				SELECT
					session_id,
					request_id,
					agent_nodes.agent_node.value('(database_name/STRING())`1`', 'string') AS database_name,
					agent_nodes.agent_node.value('(object_id/STRING())`1`', 'int') AS object_id,
					agent_nodes.agent_node.value('(hobt_id/STRING())`1`', 'bigint') AS hobt_id,
					agent_nodes.agent_node.value('(metadata_resource/STRING()`.=`SCHEMA``/../../metadata_class_id/STRING())`1`', 'STRING') AS schema_node
				FROM TEMP_TABLE_sessions AS s
				CROSS APPLY s.additional_info.nodes('//block_info') AS agent_nodes (agent_node)
				WHERE
					s.recursion = 1
			) AS t
			WHERE
				t.database_name IS NOT NULL
				AND
				(
					t.object_id IS NOT NULL
					OR t.hobt_id IS NOT NULL
					OR t.schema_node IS NOT NULL
				);
SELECT DISTINCT
					database_name
				FROM TEMP_TABLE_blocked_requests;
				
			OPEN blocks_cursor;
			
			FETCH NEXT FROM blocks_cursor
			INTO 
				V_database_name;
WHILE V_V_FETCH_STATUS = 0
			THEN

DECLARE EXIT HANDLER FOR SQLEXCEPTION
DO
GET DIAGNOSTICS CONDITION 1
;
					UPDATE TEMP_TABLE_blocked_requests
					SET
						query_error = 
							REPLACE
							(
								REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
								REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
								REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
									CAST(MESSAGE_TEXT COLLATE Latin1_General_Bin2 AS STRING),
									CHAR(31),'?'),CHAR(30),'?'),CHAR(29),'?'),CHAR(28),'?'),CHAR(27),'?'),CHAR(26),'?'),CHAR(25),'?'),CHAR(24),'?'),CHAR(23),'?'),CHAR(22),'?'),
									CHAR(21),'?'),CHAR(20),'?'),CHAR(19),'?'),CHAR(18),'?'),CHAR(17),'?'),CHAR(16),'?'),CHAR(15),'?'),CHAR(14),'?'),CHAR(12),'?'),
									CHAR(11),'?'),CHAR(8),'?'),CHAR(7),'?'),CHAR(6),'?'),CHAR(5),'?'),CHAR(4),'?'),CHAR(3),'?'),CHAR(2),'?'),CHAR(1),'?'),
								CHAR(0),
								''
							)
					WHERE
						database_name = V_database_name;

END WHILE;
SET V_sql_n = 
						CAST('' AS STRING) ||
						'UPDATE b ' ||
						'SET ' ||
							'b.schema_name = ' ||
								'REPLACE ' ||
								'( ' ||
									'REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE( ' ||
									'REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE( ' ||
									'REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE( ' ||
										's.name COLLATE Latin1_General_Bin2, ' ||
										'string(31),N"?"),string(30),N"?"),string(29),N"?"),string(28),N"?"),string(27),N"?"),string(26),N"?"),string(25),N"?"),string(24),N"?"),string(23),N"?"),string(22),N"?"), ' ||
										'string(21),N"?"),string(20),N"?"),string(19),N"?"),string(18),N"?"),string(17),N"?"),string(16),N"?"),string(15),N"?"),string(14),N"?"),string(12),N"?"), ' ||
										'string(11),N"?"),string(8),N"?"),string(7),N"?"),string(6),N"?"),string(5),N"?"),string(4),N"?"),string(3),N"?"),string(2),N"?"),string(1),N"?"), ' ||
									'string(0), ' ||
									'"" ' ||
								'), ' ||
							'b.object_name = ' ||
								'REPLACE ' ||
								'( ' ||
									'REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE( ' ||
									'REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE( ' ||
									'REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE( ' ||
										'o.name COLLATE Latin1_General_Bin2, ' ||
										'string(31),N"?"),string(30),N"?"),string(29),N"?"),string(28),N"?"),string(27),N"?"),string(26),N"?"),string(25),N"?"),string(24),N"?"),string(23),N"?"),string(22),N"?"), ' ||
										'string(21),N"?"),string(20),N"?"),string(19),N"?"),string(18),N"?"),string(17),N"?"),string(16),N"?"),string(15),N"?"),string(14),N"?"),string(12),N"?"), ' ||
										'string(11),N"?"),string(8),N"?"),string(7),N"?"),string(6),N"?"),string(5),N"?"),string(4),N"?"),string(3),N"?"),string(2),N"?"),string(1),N"?"), ' ||
									'string(0), ' '"" ' ||
								') ' ||
						'FROM TEMP_TABLE_blocked_requests AS b ' ||
						'LEFT OUTER JOIN ' || concat('[', V_database_name, ']') || '.sys.partitions AS p ON ' ||
							'p.hobt_id = b.hobt_id ' ||
						'LEFT OUTER JOIN ' || concat('[', V_database_name, ']') || '.sys.objects AS o ON ' ||
							'o.object_id = COALESCE(p.object_id, b.object_id) ' ||
						'LEFT OUTER JOIN ' || concat('[', V_database_name, ']') || '.sys.schemas AS s ON ' ||
							's.schema_id = COALESCE(o.schema_id, b.schema_id) ' ||
						'WHERE ' ||
							'b.database_name = V_database_name; ';
CALL
						V_sql_n,
						'V_database_name string',
						V_database_name;

				FETCH NEXT FROM blocks_cursor
				INTO
					V_database_name;
END IF;
			
			CLOSE blocks_cursor;
			DEALLOCATE blocks_cursor;
			MERGE INTO TEMP_TABLE_sessions s_TGT
USING (
SELECT * 
FROM TEMP_TABLE_sessions s
INNER JOIN TEMP_TABLE_blocked_requests b ON b.session_id = s.session_id AND b.request_id = s.request_id AND s.recursion = 1
)
ON b.schema_name IS NOT NULL AND 
COALESCE(s.session_id::string,'__NULL__') = COALESCE(s_TGT.session_id::string,'__NULL__') AND 
COALESCE(s.request_id::string,'__NULL__') = COALESCE(s_TGT.request_id::string,'__NULL__') AND 
COALESCE(s.recursion::string,'__NULL__') = COALESCE(s_TGT.recursion::string,'__NULL__')
WHEN MATCHED THEN UPDATE SET
additional_info.modify ( '
					insert <schema_name>{sql:column(`b.schema_name`)}</schema_name>
					as last
					into (/additional_info/block_info)`1`
				' );
			MERGE INTO TEMP_TABLE_sessions s_TGT
USING (
SELECT * 
FROM TEMP_TABLE_sessions s
INNER JOIN TEMP_TABLE_blocked_requests b ON b.session_id = s.session_id AND b.request_id = s.request_id AND s.recursion = 1
)
ON b.object_name IS NOT NULL AND 
COALESCE(s.session_id::string,'__NULL__') = COALESCE(s_TGT.session_id::string,'__NULL__') AND 
COALESCE(s.request_id::string,'__NULL__') = COALESCE(s_TGT.request_id::string,'__NULL__') AND 
COALESCE(s.recursion::string,'__NULL__') = COALESCE(s_TGT.recursion::string,'__NULL__')
WHEN MATCHED THEN UPDATE SET
additional_info.modify ( '
					insert <object_name>{sql:column(`b.object_name`)}</object_name>
					as last
					into (/additional_info/block_info)`1`
				' );
			MERGE INTO TEMP_TABLE_sessions s_TGT
USING (
SELECT * 
FROM TEMP_TABLE_sessions s
INNER JOIN TEMP_TABLE_blocked_requests b ON b.session_id = s.session_id AND b.request_id = s.request_id AND s.recursion = 1
)
ON b.query_error IS NOT NULL AND 
COALESCE(s.session_id::string,'__NULL__') = COALESCE(s_TGT.session_id::string,'__NULL__') AND 
COALESCE(s.request_id::string,'__NULL__') = COALESCE(s_TGT.request_id::string,'__NULL__') AND 
COALESCE(s.recursion::string,'__NULL__') = COALESCE(s_TGT.recursion::string,'__NULL__')
WHEN MATCHED THEN UPDATE SET
additional_info.modify ( '
					insert <query_error>{sql:column(`b.query_error`)}</query_error>
					as last
					into (/additional_info/block_info)`1`
				' );
END IF;
IF
			V_output_column_list LIKE '%|`program_name|`%' ESCAPE '|'
			AND V_output_column_list LIKE '%|`additional_info|`%' ESCAPE '|'
			AND V_recursion = 1
			AND DB_ID('msdb') IS NOT NULL
		THEN
			SET V_sql_n 'BEGIN TRY /*FIXME*/;
					DECLARE V_job_name string;
					SET V_job_name = NULL;
					DECLARE V_step_name string;
					SET V_step_name = NULL;

					SELECT
						V_job_name = 
							REPLACE
							(
								REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
								REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
								REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
									j.name,
									string(31),N"?"),string(30),N"?"),string(29),N"?"),string(28),N"?"),string(27),N"?"),string(26),N"?"),string(25),N"?"),string(24),N"?"),string(23),N"?"),string(22),N"?"),
									string(21),N"?"),string(20),N"?"),string(19),N"?"),string(18),N"?"),string(17),N"?"),string(16),N"?"),string(15),N"?"),string(14),N"?"),string(12),N"?"),
									string(11),N"?"),string(8),N"?"),string(7),N"?"),string(6),N"?"),string(5),N"?"),string(4),N"?"),string(3),N"?"),string(2),N"?"),string(1),N"?"),
								string(0),
								N"?"
							),
						V_step_name = 
							REPLACE
							(
								REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
								REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
								REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
									s.step_name,
									string(31),N"?"),string(30),N"?"),string(29),N"?"),string(28),N"?"),string(27),N"?"),string(26),N"?"),string(25),N"?"),string(24),N"?"),string(23),N"?"),string(22),N"?"),
									string(21),N"?"),string(20),N"?"),string(19),N"?"),string(18),N"?"),string(17),N"?"),string(16),N"?"),string(15),N"?"),string(14),N"?"),string(12),N"?"),
									string(11),N"?"),string(8),N"?"),string(7),N"?"),string(6),N"?"),string(5),N"?"),string(4),N"?"),string(3),N"?"),string(2),N"?"),string(1),N"?"),
								string(0),
								N"?"
							)
					FROM msdb.dbo.sysjobs AS j
					INNER JOIN msdb.dbo.sysjobsteps AS s ON
						j.job_id = s.job_id
					WHERE
						j.job_id = V_job_id
						AND s.step_id = V_step_id;

					IF V_job_name IS NOT NULL
					BEGIN

						MERGE INTO s s_TGT
USING (
SELECT * 
FROM TEMP_TABLE_sessions s
FROM TEMP_TABLE_sessions s
FROM TEMP_TABLE_sessions AS s
)
ON s.session_id = V_session_id AND s.recursion = 1  MERGE INTO s s_TGT
USING (
SELECT * 
FROM TEMP_TABLE_sessions s
FROM TEMP_TABLE_sessions s
)
ON s.session_id = V_session_id AND s.recursion = 1  END END TRY /*FIXME*/ BEGIN CATCH /*FIXME*/ DECLARE V_msdb_error_message STRING SET V_msdb_error_message = MESSAGE_TEXT MERGE INTO TEMP_TABLE_sessions s_TGT
USING TEMP_TABLE_sessions s
ON s.session_id = V_session_id AND s.recursion = 1  END CATCH /*FIXME*/ '     ; AND 
COALESCE(s.session_id::string,'__NULL__') = COALESCE(s_TGT.session_id::string,'__NULL__') AND 
COALESCE(s.recursion::string,'__NULL__') = COALESCE(s_TGT.recursion::string,'__NULL__') WHEN MATCHED THEN UPDATE SET additional_info.modify ( " insert STRING sql:variable ( `V_job_name` ) into ( / additional_info / agent_job_info / job_name ) `1` " ) additional_info.modify ( " insert STRING sql:variable ( `V_step_name` ) into ( / additional_info / agent_job_info / step_name ) `1` " ) additional_info.modify ( " insert < msdb_query_error > sql:variable ( `V_msdb_error_message` ) < / msdb_query_error > as last into ( / additional_info / agent_job_info ) `1` " ) V_msdb_error_message = MESSAGE_TEXT UPDATE s; AND 
COALESCE(s.session_id::string,'__NULL__') = COALESCE(s_TGT.session_id::string,'__NULL__') AND 
COALESCE(s.recursion::string,'__NULL__') = COALESCE(s_TGT.recursion::string,'__NULL__') WHEN MATCHED THEN UPDATE SET additional_info.modify ( " insert STRING sql:variable ( `V_step_name` ) into ( / additional_info / agent_job_info / step_name ) `1` " ) additional_info.modify ( " insert < msdb_query_error > sql:variable ( `V_msdb_error_message` ) < / msdb_query_error > as last into ( / additional_info / agent_job_info ) `1` " ) V_msdb_error_message = MESSAGE_TEXT UPDATE s; AND 
COALESCE(s.session_id::string,'__NULL__') = COALESCE(s_TGT.session_id::string,'__NULL__') AND 
COALESCE(s.recursion::string,'__NULL__') = COALESCE(s_TGT.recursion::string,'__NULL__')
WHEN MATCHED THEN UPDATE SET
additional_info.modify ( " insert < msdb_query_error > sql:variable ( `V_msdb_error_message` ) < / msdb_query_error > as last into ( / additional_info / agent_job_info ) `1` " );
SELECT
					s.session_id,
					agent_nodes.agent_node.value('(job_id/STRING())`1`', 'STRING') AS job_id,
					agent_nodes.agent_node.value('(step_id/STRING())`1`', 'int') AS step_id
				FROM TEMP_TABLE_sessions AS s
				CROSS APPLY s.additional_info.nodes('//agent_job_info') AS agent_nodes (agent_node)
				WHERE
					s.recursion = 1
			;
			
			OPEN agent_cursor;
			FETCH NEXT FROM agent_cursor
			INTO 
				V_session_id,
				V_job_id,
				V_step_id;
WHILE V_V_FETCH_STATUS = 0
			THEN
CALL
					V_sql_n,
					'V_job_id STRING, V_step_id INT, V_session_id SMALLINT',
					V_job_id, V_step_id, V_session_id

				FETCH NEXT FROM agent_cursor
				INTO 
					V_session_id,
					V_job_id,
					V_step_id;
END IF;
			CLOSE agent_cursor;
			DEALLOCATE agent_cursor;
END IF;
IF 
			V_delta_interval > 0 
			AND V_recursion <> 1
		THEN
SET V_recursion = 1;
SET V_delay_time = date_format(DATEADD(second, V_delta_interval, 0), 'hh:mm:ss:SSS');
			WAITFOR DELAY V_delay_time;
			GOTO REDO;
END IF;
END IF;
SET V_sql = 
		--Outer column list
		CAST(CASE
WHEN 
V_destination_table <> '' 
AND V_return_schema = 0 
THEN 'INSERT ' + V_destination_table + ' '
ELSE ''
END ||
'SELECT ' ||
V_output_column_list || ' ' ||
CASE V_return_schema
WHEN 1 THEN 'INTO #session_schema '
ELSE ''
END
--End outer column list AS STRING) + 
		--Inner column list
		CAST('FROM ' ||
'( ' ||
'SELECT ' ||
'session_id, ' ||
--[dd hh:mm:ss.mss]
CASE
WHEN V_format_output IN (1, 2) THEN
'CASE ' +
'WHEN elapsed_time < 0 THEN ' +
'RIGHT ' +
'( ' +
'REPLICATE("0", max_elapsed_length) + CONVERT(VARCHAR, (-1 * elapsed_time) / 86400), ' +
'max_elapsed_length ' +
') + ' +
'RIGHT ' +
'( ' +
'CONVERT(VARCHAR, DATEADD(second, (-1 * elapsed_time), 0), 120), ' +
'9 ' +
') + ' +
'".000" ' +
'ELSE ' +
'RIGHT ' +
'( ' +
'REPLICATE("0", max_elapsed_length) + CONVERT(VARCHAR, elapsed_time / 86400000), ' +
'max_elapsed_length ' +
') + ' +
'RIGHT ' +
'( ' +
'CONVERT(VARCHAR, DATEADD(second, elapsed_time / 1000, 0), 120), ' +
'9 ' +
') + ' +
'"."|| ' + 
'RIGHT("000"|| CONVERT(VARCHAR, elapsed_time % 1000), 3) ' +
'END AS [dd hh:mm:ss.mss], '
ELSE
''
END +
--[dd hh:mm:ss.mss (avg)] / avg_elapsed_time
CASE 
WHEN  V_format_output IN (1, 2) THEN 
'RIGHT ' +
'( ' +
'"00"|| CONVERT(VARCHAR, avg_elapsed_time / 86400000), ' +
'2 ' +
') + ' +
'RIGHT ' +
'( ' +
'CONVERT(VARCHAR, DATEADD(second, avg_elapsed_time / 1000, 0), 120), ' +
'9 ' +
') + ' +
'"."|| ' +
'RIGHT("000"|| CONVERT(VARCHAR, avg_elapsed_time % 1000), 3) AS [dd hh:mm:ss.mss (avg)], '
ELSE
'avg_elapsed_time, '
END +
--physical_io
CASE V_format_output
WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, physical_io))) OVER() - LEN(CONVERT(VARCHAR, physical_io))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, physical_io), 1), 19)) AS '
WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, physical_io), 1), 19)) AS '
ELSE ''
END || 'physical_io, ' ||
--reads
CASE V_format_output
WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, reads))) OVER() - LEN(CONVERT(VARCHAR, reads))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, reads), 1), 19)) AS '
WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, reads), 1), 19)) AS '
ELSE ''
END || 'reads, ' ||
--physical_reads
CASE V_format_output
WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, physical_reads))) OVER() - LEN(CONVERT(VARCHAR, physical_reads))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, physical_reads), 1), 19)) AS '
WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, physical_reads), 1), 19)) AS '
ELSE ''
END || 'physical_reads, ' ||
--writes
CASE V_format_output
WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, writes))) OVER() - LEN(CONVERT(VARCHAR, writes))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, writes), 1), 19)) AS '
WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, writes), 1), 19)) AS '
ELSE ''
END || 'writes, ' ||
--tempdb_allocations
CASE V_format_output
WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, tempdb_allocations))) OVER() - LEN(CONVERT(VARCHAR, tempdb_allocations))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, tempdb_allocations), 1), 19)) AS '
WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, tempdb_allocations), 1), 19)) AS '
ELSE ''
END || 'tempdb_allocations, ' ||
--tempdb_current
CASE V_format_output
WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, tempdb_current))) OVER() - LEN(CONVERT(VARCHAR, tempdb_current))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, tempdb_current), 1), 19)) AS '
WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, tempdb_current), 1), 19)) AS '
ELSE ''
END || 'tempdb_current, ' ||
--CPU
CASE V_format_output
WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, CPU))) OVER() - LEN(CONVERT(VARCHAR, CPU))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, CPU), 1), 19)) AS '
WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, CPU), 1), 19)) AS '
ELSE ''
END || 'CPU, ' ||
--context_switches
CASE V_format_output
WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, context_switches))) OVER() - LEN(CONVERT(VARCHAR, context_switches))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, context_switches), 1), 19)) AS '
WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, context_switches), 1), 19)) AS '
ELSE ''
END || 'context_switches, ' ||
--used_memory
CASE V_format_output
WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, used_memory))) OVER() - LEN(CONVERT(VARCHAR, used_memory))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, used_memory), 1), 19)) AS '
WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, used_memory), 1), 19)) AS '
ELSE ''
END || 'used_memory, ' ||
CASE
WHEN V_output_column_list LIKE '%|_delta|]%' ESCAPE '|' THEN
--physical_io_delta			
'CASE ' +
'WHEN ' +
'first_request_start_time = last_request_start_time ' + 
'AND num_events = 2 ' +
'AND physical_io_delta >= 0 ' +
'THEN ' +
CASE V_format_output
WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, physical_io_delta))) OVER() - LEN(CONVERT(VARCHAR, physical_io_delta))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, physical_io_delta), 1), 19)) ' 
WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, physical_io_delta), 1), 19)) '
ELSE 'physical_io_delta '
END +
'ELSE NULL ' +
'END AS physical_io_delta, ' +
--reads_delta
'CASE ' +
'WHEN ' +
'first_request_start_time = last_request_start_time ' + 
'AND num_events = 2 ' +
'AND reads_delta >= 0 ' +
'THEN ' +
CASE V_format_output
WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, reads_delta))) OVER() - LEN(CONVERT(VARCHAR, reads_delta))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, reads_delta), 1), 19)) '
WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, reads_delta), 1), 19)) '
ELSE 'reads_delta '
END +
'ELSE NULL ' +
'END AS reads_delta, ' +
--physical_reads_delta
'CASE ' +
'WHEN ' +
'first_request_start_time = last_request_start_time ' + 
'AND num_events = 2 ' +
'AND physical_reads_delta >= 0 ' +
'THEN ' +
CASE V_format_output
WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, physical_reads_delta))) OVER() - LEN(CONVERT(VARCHAR, physical_reads_delta))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, physical_reads_delta), 1), 19)) '
WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, physical_reads_delta), 1), 19)) '
ELSE 'physical_reads_delta '
END + 
'ELSE NULL ' +
'END AS physical_reads_delta, ' +
--writes_delta
'CASE ' +
'WHEN ' +
'first_request_start_time = last_request_start_time ' + 
'AND num_events = 2 ' +
'AND writes_delta >= 0 ' +
'THEN ' +
CASE V_format_output
WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, writes_delta))) OVER() - LEN(CONVERT(VARCHAR, writes_delta))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, writes_delta), 1), 19)) '
WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, writes_delta), 1), 19)) '
ELSE 'writes_delta '
END + 
'ELSE NULL ' +
'END AS writes_delta, ' +
--tempdb_allocations_delta
'CASE ' +
'WHEN ' +
'first_request_start_time = last_request_start_time ' + 
'AND num_events = 2 ' +
'AND tempdb_allocations_delta >= 0 ' +
'THEN ' +
CASE V_format_output
WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, tempdb_allocations_delta))) OVER() - LEN(CONVERT(VARCHAR, tempdb_allocations_delta))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, tempdb_allocations_delta), 1), 19)) '
WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, tempdb_allocations_delta), 1), 19)) '
ELSE 'tempdb_allocations_delta '
END + 
'ELSE NULL ' +
'END AS tempdb_allocations_delta, ' +
--tempdb_current_delta
--this is the only one that can (legitimately) go negative 
'CASE ' +
'WHEN ' +
'first_request_start_time = last_request_start_time ' + 
'AND num_events = 2 ' +
'THEN ' +
CASE V_format_output
WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, tempdb_current_delta))) OVER() - LEN(CONVERT(VARCHAR, tempdb_current_delta))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, tempdb_current_delta), 1), 19)) '
WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, tempdb_current_delta), 1), 19)) '
ELSE 'tempdb_current_delta '
END + 
'ELSE NULL ' +
'END AS tempdb_current_delta, ' +
--CPU_delta
'CASE ' +
'WHEN ' +
'first_request_start_time = last_request_start_time ' + 
'AND num_events = 2 ' +
'THEN ' +
'CASE ' +
'WHEN ' +
'thread_CPU_delta > CPU_delta ' +
'AND thread_CPU_delta > 0 ' +
'THEN ' +
CASE V_format_output
WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, thread_CPU_delta + CPU_delta))) OVER() - LEN(CONVERT(VARCHAR, thread_CPU_delta))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, thread_CPU_delta), 1), 19)) '
WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, thread_CPU_delta), 1), 19)) '
ELSE 'thread_CPU_delta '
END + 
'WHEN CPU_delta >= 0 THEN ' +
CASE V_format_output
WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, thread_CPU_delta + CPU_delta))) OVER() - LEN(CONVERT(VARCHAR, CPU_delta))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, CPU_delta), 1), 19)) '
WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, CPU_delta), 1), 19)) '
ELSE 'CPU_delta '
END + 
'ELSE NULL ' +
'END ' +
'ELSE ' +
'NULL ' +
'END AS CPU_delta, ' +
--context_switches_delta
'CASE ' +
'WHEN ' +
'first_request_start_time = last_request_start_time ' + 
'AND num_events = 2 ' +
'AND context_switches_delta >= 0 ' +
'THEN ' +
CASE V_format_output
WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, context_switches_delta))) OVER() - LEN(CONVERT(VARCHAR, context_switches_delta))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, context_switches_delta), 1), 19)) '
WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, context_switches_delta), 1), 19)) '
ELSE 'context_switches_delta '
END + 
'ELSE NULL ' +
'END AS context_switches_delta, ' +
--used_memory_delta
'CASE ' +
'WHEN ' +
'first_request_start_time = last_request_start_time ' + 
'AND num_events = 2 ' +
'AND used_memory_delta >= 0 ' +
'THEN ' +
CASE V_format_output
WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, used_memory_delta))) OVER() - LEN(CONVERT(VARCHAR, used_memory_delta))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, used_memory_delta), 1), 19)) '
WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, used_memory_delta), 1), 19)) '
ELSE 'used_memory_delta '
END + 
'ELSE NULL ' +
'END AS used_memory_delta, '
ELSE ''
END +
--tasks
CASE V_format_output
WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, tasks))) OVER() - LEN(CONVERT(VARCHAR, tasks))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, tasks), 1), 19)) AS '
WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, tasks), 1), 19)) '
ELSE ''
END || 'tasks, ' ||
'status, ' ||
'wait_info, ' ||
'locks, ' ||
'tran_start_time, ' ||
'LEFT(tran_log_writes, LEN(tran_log_writes) - 1) AS tran_log_writes, ' ||
--open_tran_count
CASE V_format_output
WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, open_tran_count))) OVER() - LEN(CONVERT(VARCHAR, open_tran_count))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, open_tran_count), 1), 19)) AS '
WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, open_tran_count), 1), 19)) AS '
ELSE ''
END || 'open_tran_count, ' ||
--sql_command
CASE V_format_output 
WHEN 0 THEN 'REPLACE(REPLACE(CONVERT(NVARCHAR(MAX), sql_command), "<?query --"||CHAR(13)+CHAR(10), ""), CHAR(13)+CHAR(10)||"--?>", "") AS '
ELSE ''
END || 'sql_command, ' ||
--sql_text
CASE V_format_output 
WHEN 0 THEN 'REPLACE(REPLACE(CONVERT(NVARCHAR(MAX), sql_text), "<?query --"||CHAR(13)+CHAR(10), ""), CHAR(13)+CHAR(10)||"--?>", "") AS '
ELSE ''
END || 'sql_text, ' ||
'query_plan, ' ||
'blocking_session_id, ' ||
--blocked_session_count
CASE V_format_output
WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, blocked_session_count))) OVER() - LEN(CONVERT(VARCHAR, blocked_session_count))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, blocked_session_count), 1), 19)) AS '
WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, blocked_session_count), 1), 19)) AS '
ELSE ''
END || 'blocked_session_count, ' ||
--percent_complete
CASE V_format_output
WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, CONVERT(MONEY, percent_complete), 2))) OVER() - LEN(CONVERT(VARCHAR, CONVERT(MONEY, percent_complete), 2))) + CONVERT(CHAR(22), CONVERT(MONEY, percent_complete), 2)) AS '
WHEN 2 THEN 'CONVERT(VARCHAR, CONVERT(CHAR(22), CONVERT(MONEY, blocked_session_count), 1)) AS '
ELSE ''
END || 'percent_complete, ' ||
'host_name, ' ||
'login_name, ' ||
'database_name, ' ||
'program_name, ' ||
'additional_info, ' ||
'start_time, ' ||
'login_time, ' ||
'CASE ' ||
'WHEN status "sleeping" THEN NULL ' ||
'ELSE request_id ' ||
'END AS request_id, ' ||
'current_timestamp() AS collection_time '
--End inner column list AS STRING) +
		--Derived table and INSERT specification
		CAST('FROM ' ||
'( ' ||
'SELECT TOP(2147483647) ' ||
'*, ' ||
'CASE ' ||
'MAX ' ||
'( ' ||
'LEN ' ||
'( ' ||
'CONVERT ' ||
'( ' ||
'STRING, ' ||
'CASE ' ||
'WHEN elapsed_time < 0 THEN ' ||
'(-1 * elapsed_time) / 86400 ' ||
'ELSE ' ||
'elapsed_time / 86400000 ' ||
'END ' ||
') ' ||
') ' ||
') OVER () ' ||
'WHEN 1 THEN 2 ' ||
'ELSE ' ||
'MAX ' ||
'( ' ||
'LEN ' ||
'( ' ||
'CONVERT ' ||
'( ' ||
'STRING, ' ||
'CASE ' ||
'WHEN elapsed_time < 0 THEN ' ||
'(-1 * elapsed_time) / 86400 ' ||
'ELSE ' ||
'elapsed_time / 86400000 ' ||
'END ' ||
') ' ||
') ' ||
') OVER () ' ||
'END AS max_elapsed_length, ' ||
CASE
WHEN V_output_column_list LIKE '%|_delta|]%' ESCAPE '|' THEN
'MAX(physical_io * recursion) OVER (PARTITION BY session_id, request_id) + ' +
'MIN(physical_io * recursion) OVER (PARTITION BY session_id, request_id) AS physical_io_delta, ' +
'MAX(reads * recursion) OVER (PARTITION BY session_id, request_id) + ' +
'MIN(reads * recursion) OVER (PARTITION BY session_id, request_id) AS reads_delta, ' +
'MAX(physical_reads * recursion) OVER (PARTITION BY session_id, request_id) + ' +
'MIN(physical_reads * recursion) OVER (PARTITION BY session_id, request_id) AS physical_reads_delta, ' +
'MAX(writes * recursion) OVER (PARTITION BY session_id, request_id) + ' +
'MIN(writes * recursion) OVER (PARTITION BY session_id, request_id) AS writes_delta, ' +
'MAX(tempdb_allocations * recursion) OVER (PARTITION BY session_id, request_id) + ' +
'MIN(tempdb_allocations * recursion) OVER (PARTITION BY session_id, request_id) AS tempdb_allocations_delta, ' +
'MAX(tempdb_current * recursion) OVER (PARTITION BY session_id, request_id) + ' +
'MIN(tempdb_current * recursion) OVER (PARTITION BY session_id, request_id) AS tempdb_current_delta, ' +
'MAX(CPU * recursion) OVER (PARTITION BY session_id, request_id) + ' +
'MIN(CPU * recursion) OVER (PARTITION BY session_id, request_id) AS CPU_delta, ' +
'MAX(thread_CPU_snapshot * recursion) OVER (PARTITION BY session_id, request_id) + ' +
'MIN(thread_CPU_snapshot * recursion) OVER (PARTITION BY session_id, request_id) AS thread_CPU_delta, ' +
'MAX(context_switches * recursion) OVER (PARTITION BY session_id, request_id) + ' +
'MIN(context_switches * recursion) OVER (PARTITION BY session_id, request_id) AS context_switches_delta, ' +
'MAX(used_memory * recursion) OVER (PARTITION BY session_id, request_id) + ' +
'MIN(used_memory * recursion) OVER (PARTITION BY session_id, request_id) AS used_memory_delta, ' +
'MIN(last_request_start_time) OVER (PARTITION BY session_id, request_id) AS first_request_start_time, '
ELSE ''
END ||
'COUNT(*) OVER (PARTITION BY session_id, request_id) AS num_events ' ||
'FROM TEMP_TABLE_sessions AS s1 ' ||
CASE 
WHEN V_sort_order = '' THEN ''
ELSE
'ORDER BY ' +
V_sort_order
END ||
') AS s ' ||
'WHERE ' ||
's.recursion = 1 ' ||
') x ' ||
'; ' ||
'' ||
CASE V_return_schema
WHEN 1 THEN
'SET V_schema = ' +
'"CREATE TABLE <table_name> ( "|| ' +
'STUFF ' +
'( ' +
'( ' +
'SELECT ' +
'","|| ' +
'QUOTENAME(COLUMN_NAME) || " "|| ' +
'DATA_TYPE + ' + 
'CASE ' +
'WHEN DATA_TYPE LIKE "%char" THEN "("|| COALESCE(NULLIF(CONVERT(VARCHAR, CHARACTER_MAXIMUM_LENGTH), "-1"), "max") || ") " ' +
'ELSE " " ' +
'END + ' +
'CASE IS_NULLABLE ' +
'WHEN "NO" THEN "NOT " ' +
'ELSE "" ' +
'END || "NULL" AS [text()] ' +
'FROM tempdb.INFORMATION_SCHEMA.COLUMNS ' +
'WHERE ' +
'TABLE_NAME = (SELECT name FROM tempdb.sys.objects WHERE object_id = OBJECT_ID("tempdb..#session_schema")) ' +
'ORDER BY ' +
'ORDINAL_POSITION ' +
'FOR XML ' +
'PATH("") ' +
'), + ' +
'1, ' +
'1, ' +
'"" ' +
') + ' +
'")"; ' 
ELSE ''
END
--End derived table and INSERT specification AS STRING);
SET V_sql_n = CAST(V_sql AS STRING);
CALL
		V_sql_n,
		'V_schema STRING ',
		V_schema ;
END IF;

END IF
