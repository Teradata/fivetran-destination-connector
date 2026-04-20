-- Validation for schema_migrations_input_sync_modes.json
-- Exercises: add_column_in_history_mode, drop_column_in_history_mode,
--            copy_table_to_history_mode, migrate_soft_delete_to_history,
--            migrate_history_to_soft_delete

-- transaction: migrated soft_delete -> history, must have history columns AND no _fivetran_deleted
SELECT CASE WHEN COUNT(*) = 3 THEN 'PASS: transaction has history columns after soft_delete->history migration'
            ELSE 'FAIL: transaction missing history columns (count='||TRIM(COUNT(*))||')'
       END (TITLE '')
FROM DBC.ColumnsV
WHERE DatabaseName = DATABASE AND TableName = 'transaction'
  AND ColumnName IN ('_fivetran_start','_fivetran_end','_fivetran_active');

SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS: transaction._fivetran_deleted removed after migration to history'
            ELSE 'FAIL: transaction._fivetran_deleted still present'
       END (TITLE '')
FROM DBC.ColumnsV
WHERE DatabaseName = DATABASE AND TableName = 'transaction' AND ColumnName = '_fivetran_deleted';

-- transaction_history: add_column_in_history_mode / drop_column_in_history_mode applied
SELECT CASE WHEN COUNT(*) = 3 THEN 'PASS: transaction_history retains history columns'
            ELSE 'FAIL: transaction_history history columns missing (count='||TRIM(COUNT(*))||')'
       END (TITLE '')
FROM DBC.ColumnsV
WHERE DatabaseName = DATABASE AND TableName = 'transaction_history'
  AND ColumnName IN ('_fivetran_start','_fivetran_end','_fivetran_active');

-- new_transaction_history: migrated history -> soft_delete; must have _fivetran_deleted, no history cols
SELECT CASE WHEN COUNT(*) = 1 THEN 'PASS: new_transaction_history has _fivetran_deleted after history->soft_delete'
            ELSE 'FAIL: new_transaction_history._fivetran_deleted missing'
       END (TITLE '')
FROM DBC.ColumnsV
WHERE DatabaseName = DATABASE AND TableName = 'new_transaction_history' AND ColumnName = '_fivetran_deleted';

SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS: new_transaction_history history columns removed'
            ELSE 'FAIL: new_transaction_history still has '||TRIM(COUNT(*))||' history column(s)'
       END (TITLE '')
FROM DBC.ColumnsV
WHERE DatabaseName = DATABASE AND TableName = 'new_transaction_history'
  AND ColumnName IN ('_fivetran_start','_fivetran_end','_fivetran_active');
