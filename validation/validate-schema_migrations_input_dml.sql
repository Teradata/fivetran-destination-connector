-- Validation for schema_migrations_input_dml.json
-- Exercises: copy_column, update_column_value, add_column_with_default_value,
--            set_column_to_null, copy_table, rename_column, rename_table, drop_table

-- transaction_drop: dropped, must not exist
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS: transaction_drop dropped'
            ELSE 'FAIL: transaction_drop still exists'
       END (TITLE '')
FROM DBC.TablesV
WHERE DatabaseName = DATABASE AND TableName = 'transaction_drop' AND TableKind = 'T';

-- transaction_new: renamed away to transaction_renamed, must not exist
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS: transaction_new renamed away'
            ELSE 'FAIL: transaction_new still exists (rename_table did not take effect)'
       END (TITLE '')
FROM DBC.TablesV
WHERE DatabaseName = DATABASE AND TableName = 'transaction_new' AND TableKind = 'T';

-- transaction_renamed: exists
SELECT CASE WHEN COUNT(*) = 1 THEN 'PASS: transaction_renamed exists'
            ELSE 'FAIL: transaction_renamed missing'
       END (TITLE '')
FROM DBC.TablesV
WHERE DatabaseName = DATABASE AND TableName = 'transaction_renamed' AND TableKind = 'T';

-- transaction: source of copy_table — must still exist
SELECT CASE WHEN COUNT(*) = 1 THEN 'PASS: transaction (source) still exists after copy_table'
            ELSE 'FAIL: transaction missing'
       END (TITLE '')
FROM DBC.TablesV
WHERE DatabaseName = DATABASE AND TableName = 'transaction' AND TableKind = 'T';

-- transaction: rename_column happened (old column name gone, new column name present)
-- The ddl renames "old_column" -> "renamed_column"
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS: transaction.old_column renamed away'
            ELSE 'FAIL: transaction.old_column still present'
       END (TITLE '')
FROM DBC.ColumnsV
WHERE DatabaseName = DATABASE AND TableName = 'transaction' AND ColumnName = 'old_column';

SELECT CASE WHEN COUNT(*) = 1 THEN 'PASS: transaction.renamed_column present'
            ELSE 'FAIL: transaction.renamed_column missing'
       END (TITLE '')
FROM DBC.ColumnsV
WHERE DatabaseName = DATABASE AND TableName = 'transaction' AND ColumnName = 'renamed_column';
