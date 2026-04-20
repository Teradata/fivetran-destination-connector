-- Validation for canonical input.json
-- PASS rows are informational; any row beginning "FAIL:" fails the check.
-- Assumes tables live in the database set by the runner (DATABASE "<db>"; already issued).

-- transaction: created in history mode, so must have history columns
SELECT CASE WHEN COUNT(*) = 3 THEN 'PASS: transaction has all 3 history columns'
            ELSE 'FAIL: transaction missing history columns (count='||TRIM(COUNT(*))||')'
       END (TITLE '')
FROM DBC.ColumnsV
WHERE DatabaseName = DATABASE AND TableName = 'transaction'
  AND ColumnName IN ('_fivetran_start','_fivetran_end','_fivetran_active');

-- transaction: alter_table added new_column, dropped old_column
SELECT CASE WHEN COUNT(*) = 1 THEN 'PASS: transaction.new_column present'
            ELSE 'FAIL: transaction.new_column missing'
       END (TITLE '')
FROM DBC.ColumnsV
WHERE DatabaseName = DATABASE AND TableName = 'transaction' AND ColumnName = 'new_column';

SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS: transaction.old_column dropped'
            ELSE 'FAIL: transaction.old_column still present'
       END (TITLE '')
FROM DBC.ColumnsV
WHERE DatabaseName = DATABASE AND TableName = 'transaction' AND ColumnName = 'old_column';

-- campaign: was truncated, must be empty
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS: campaign is empty after truncate_before'
            ELSE 'FAIL: campaign has '||TRIM(COUNT(*))||' rows, expected 0'
       END (TITLE '')
FROM "campaign";

-- composite_table: PK column renamed from old_pk -> new_pk
SELECT CASE WHEN COUNT(*) = 1 THEN 'PASS: composite_table.new_pk present'
            ELSE 'FAIL: composite_table.new_pk missing'
       END (TITLE '')
FROM DBC.ColumnsV
WHERE DatabaseName = DATABASE AND TableName = 'composite_table' AND ColumnName = 'new_pk';

SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS: composite_table.old_pk dropped'
            ELSE 'FAIL: composite_table.old_pk still present'
       END (TITLE '')
FROM DBC.ColumnsV
WHERE DatabaseName = DATABASE AND TableName = 'composite_table' AND ColumnName = 'old_pk';

-- composite_table: soft_truncate_before was issued -> every row should have _fivetran_deleted
SELECT CASE WHEN COUNT(*) >= 1 THEN 'PASS: composite_table has _fivetran_deleted column'
            ELSE 'FAIL: composite_table._fivetran_deleted missing (expected after soft_truncate)'
       END (TITLE '')
FROM DBC.ColumnsV
WHERE DatabaseName = DATABASE AND TableName = 'composite_table' AND ColumnName = '_fivetran_deleted';
