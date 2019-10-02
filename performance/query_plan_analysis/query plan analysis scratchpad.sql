/* query plan analysis scratchpad

	- found base select statement online
	- customized for my preferences
		- formatted for better readability
		- added many more columns
		- used in conjunction with SentryOne Plan Explorer (free) to tune the worst offenders
		- used to also find and drop plans that need to be replaced
*/

declare
		@onOrAfter_dt datetime = dateadd(HOUR, -1, getdate())
		, @dbName sysname = db_name(db_id())

if 1=1 /* query plan analysis */
begin
	select top 1000 
		query_hash
		, query_plan_hash
		, sample_sql_handle
		--, sample_sql_handleb
		, sample_plan_handle
		, d_last_dop
		, d_last_grant_kb
		, d_last_used_grant_kb
		, d_last_grant_kb - d_last_used_grant_kb as last_unused_grant_kb
		--, d_last_spills
		, d_last_execution_time
		, d_creation_time
		, d_max_dop
		--, d_max_spills
		, d_max_grant_kb
		, d_max_used_grant_kb
		, d_max_logical_reads
		, cached_plan_object_count
		, execution_count
		, total_cpu_time_ms
		, total_elapsed_time_ms
		, total_logical_reads
		, total_logical_writes
		, total_physical_reads
		, sample_database_name
		, sample_object_name
		, sample_statement_text
	from 
	(
		select 
				query_hash
				, query_plan_hash
				, COUNT(*) as cached_plan_object_count
				, MAX(plan_handle) as sample_plan_handle
				--, MAX(sql_handle) as sample_sql_handleb
				, SUM(execution_count) as execution_count
				, SUM(total_worker_time) / 1000 as total_cpu_time_ms
				, SUM(total_elapsed_time) / 1000 as total_elapsed_time_ms
				, SUM(total_logical_reads) as total_logical_reads
				, SUM(total_logical_writes) as total_logical_writes
				, SUM(total_physical_reads) as total_physical_reads 
				, MAX(last_dop) as d_last_dop
				, MAX(last_grant_kb) as d_last_grant_kb
				, MAX(last_used_grant_kb) as d_last_used_grant_kb
				, MAX(max_dop) as d_max_dop
				, MAX(max_grant_kb) as d_max_grant_kb
				, MAX(max_used_grant_kb) as d_max_used_grant_kb
				--, MAX(last_spills) as d_last_spills
				, 0 as d_last_spills -- too old for d_last_spills
				--, MAX(max_spills) as d_max_spills
				, 0 as d_max_spills -- too old for d_max_spills
				, MAX(max_logical_reads) as d_max_logical_reads
				, MAX(last_execution_time) as d_last_execution_time
				, MIN(creation_time) as d_creation_time
			from sys.dm_exec_query_stats
			group by query_hash
				, query_plan_hash
	) as plan_hash_stats
	cross apply 
	(
		select top 1 
				qs.sql_handle as sample_sql_handle
				, qs.statement_start_offset as sample_statement_start_offset
				, qs.statement_end_offset as sample_statement_end_offset
				, case 
					when [database_id].value = 32768
						then 'ResourceDb'
					else DB_NAME(CONVERT(int, [database_id].value))
				end as sample_database_name
				, OBJECT_NAME(CONVERT(int, [object_id].value), CONVERT(int, [database_id].value)) as sample_object_name
				, SUBSTRING(sql.text, (qs.statement_start_offset / 2) + 1
					, (
						(
							case qs.statement_end_offset
								when - 1
									then DATALENGTH(sql.text)
								when 0
									then DATALENGTH(sql.text)
								else qs.statement_end_offset
								end - qs.statement_start_offset
						) / 2
					) + 1) as sample_statement_text
			from sys.dm_exec_sql_text(plan_hash_stats.sample_plan_handle) as sql
				inner join sys.dm_exec_query_stats as qs on qs.plan_handle = plan_hash_stats.sample_plan_handle
				cross apply sys.dm_exec_plan_attributes(plan_hash_stats.sample_plan_handle) as [object_id]
				cross apply sys.dm_exec_plan_attributes(plan_hash_stats.sample_plan_handle) as [database_id]
			where	[object_id].attribute = 'objectid'
					and [database_id].attribute = 'dbid'
	) as sample_query_text
	where	sample_database_name = @dbName
			and d_last_execution_time >= @onOrAfter_dt
	--where	sample_object_name = 'client_stti_Individual_DuplicateCheck'
	--where	(d_max_spills > 0)
	--order by d_max_spills desc
	--order by total_cpu_time_ms desc;
	order by last_unused_grant_kb desc

/* -- * use care when dropping plans and especially when dropping the whole cache!

	dbcc freeproccache [ ( { plan_handle | sql_handle | pool_name } ) ] [ WITH NO_INFOMSGS ]
--	dbcc freeproccache (0x0600050054D0AB08D01EC36FF101000001000000000000000000000000000000000000000000000000000000)
--	dbcc freeproccache (0x06000600FFFF240C90F1365A0700000001000000000000000000000000000000000000000000000000000000)
	dbcc freeproccache (0x06000600D1EBB101F07B544E0700000001000000000000000000000000000000000000000000000000000000)

*/


end
