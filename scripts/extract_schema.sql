-- Dump per-column metadata (type + domain + nullability) for a schema, one row
-- per column. Export to columns.csv (header row included), then feed it +
-- the constraints.csv from extract_constraints.sql to
-- scripts/build_schema_from_dump.py to (re)generate schema.json.
--
-- SQL Developer: run, then right-click the grid -> Export -> CSV (include header).
-- sqlplus (Oracle 12.2+):
--   SET PAGESIZE 0 FEEDBACK OFF
--   SET MARKUP CSV ON QUOTE OFF
--   SPOOL columns.csv
--   @scripts/extract_schema.sql
--   SPOOL OFF
--
-- Change the owner below to your target schema.

SELECT tc.table_name     AS table_name,
       tc.column_name    AS column_name,
       tc.data_type      AS data_type,
       tc.data_precision AS data_precision,  -- NUMBER precision (null if unconstrained)
       tc.data_scale     AS data_scale,      -- NUMBER scale
       tc.char_length    AS char_length,     -- CHAR/VARCHAR length in chars
       tc.nullable       AS nullable         -- 'Y' nullable, 'N' NOT NULL
FROM   all_tab_columns tc
WHERE  tc.owner = 'CETIP'
ORDER  BY tc.table_name, tc.column_id;
