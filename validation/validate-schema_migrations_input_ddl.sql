-- Validation for schema_migrations_input_ddl.json
-- Exercises: add_column, change_column_data_type, drop_column on "transaction"

-- transaction: new_column added
SELECT CASE WHEN COUNT(*) = 1 THEN 'PASS: transaction.new_column added'
            ELSE 'FAIL: transaction.new_column missing'
       END (TITLE '')
FROM DBC.ColumnsV
WHERE DatabaseName = DATABASE AND TableName = 'transaction' AND ColumnName = 'new_column';

-- transaction: old_column dropped
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS: transaction.old_column dropped'
            ELSE 'FAIL: transaction.old_column still present'
       END (TITLE '')
FROM DBC.ColumnsV
WHERE DatabaseName = DATABASE AND TableName = 'transaction' AND ColumnName = 'old_column';

-- transaction: amount_decimal scale changed (precision/scale differs after change_column_data_type)
-- The ddl input changes scale from 10 -> 15 for amount_decimal; DecimalFractionalDigits should be 15
SELECT CASE WHEN DecimalFractionalDigits = 15 THEN 'PASS: transaction.amount_decimal scale=15'
            ELSE 'FAIL: transaction.amount_decimal scale='||TRIM(DecimalFractionalDigits)||' (expected 15)'
       END (TITLE '')
FROM DBC.ColumnsV
WHERE DatabaseName = DATABASE AND TableName = 'transaction' AND ColumnName = 'amount_decimal';
