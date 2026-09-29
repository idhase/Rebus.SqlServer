-- Recreates the SqlServerTransport receive index on every queue table in the current database that still has the old
-- ([priority] ASC, [visible], [expiration], [id]) layout, as ([priority] DESC, [visible], [id], [expiration]).
-- Lease transport indexes (IDX_RECEIVE_LEASE_*) are not touched.
--
-- Run it in each database with queue tables. Recreating an index locks its table briefly, so pick a quiet moment.
-- The script only prints the statements. Review them, then uncomment the EXEC line and run it again.

DECLARE @sql nvarchar(max) = N'';

SELECT  @sql += N'DROP INDEX ' + QUOTENAME(i.[name]) + N' ON ' + QUOTENAME(OBJECT_SCHEMA_NAME(i.[object_id])) + N'.' + QUOTENAME(OBJECT_NAME(i.[object_id])) + N';' + CHAR(10)
             + N'CREATE NONCLUSTERED INDEX ' + QUOTENAME(i.[name]) + N' ON ' + QUOTENAME(OBJECT_SCHEMA_NAME(i.[object_id])) + N'.' + QUOTENAME(OBJECT_NAME(i.[object_id]))
             + N' ([priority] DESC, [visible] ASC, [id] ASC, [expiration] ASC);' + CHAR(10)
FROM    sys.indexes i
JOIN    sys.index_columns ic ON ic.[object_id] = i.[object_id] AND ic.[index_id] = i.[index_id] AND ic.[key_ordinal] = 1
JOIN    sys.columns c ON c.[object_id] = ic.[object_id] AND c.[column_id] = ic.[column_id]
WHERE   i.[name] LIKE N'IDX[_]RECEIVE[_]%'
AND     i.[name] NOT LIKE N'IDX[_]RECEIVE[_]LEASE[_]%'
AND     c.[name] = N'priority'
AND     ic.[is_descending_key] = 0;

PRINT @sql;
-- EXEC sp_executesql @sql;
