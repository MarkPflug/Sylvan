SELECT 
a3.name AS [schemaname],
a2.name AS [tablename],
a1.rows as row_count,
a1.data * 8 * 1024 AS data
FROM
(SELECT
ps.object_id,
SUM (
CASE
WHEN (ps.index_id < 2) THEN row_count
ELSE 0
END
) AS [rows],
SUM (
CASE
WHEN (ps.index_id < 2) THEN (ps.in_row_data_page_count + ps.lob_used_page_count + ps.row_overflow_used_page_count)
ELSE (ps.lob_used_page_count + ps.row_overflow_used_page_count)
END
) AS data,
SUM (ps.used_page_count) AS used
FROM sys.dm_db_partition_stats ps
WHERE ps.object_id NOT IN (SELECT object_id FROM sys.tables WHERE is_memory_optimized = 1)
GROUP BY ps.object_id) AS a1
			
INNER JOIN sys.all_objects a2  ON ( a1.object_id = a2.object_id )
INNER JOIN sys.schemas a3 ON (a2.schema_id = a3.schema_id)
WHERE a2.type <> N'S' and a2.type <> N'IT'
		