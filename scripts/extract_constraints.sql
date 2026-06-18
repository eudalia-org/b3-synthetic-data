-- Dump every PK / UNIQUE / FK constraint column for a schema, one row per
-- (constraint, column), with position so composite keys can be paired.
--
-- Export the result to a CSV with a header row (constraints.csv), then feed it
-- to scripts/build_specs_from_constraints.py to (re)generate specs.json.
--
-- SQL Developer: run, then right-click the grid -> Export -> CSV (include header).
-- sqlplus alternative (Oracle 12.2+):
--   SET PAGESIZE 0 FEEDBACK OFF
--   SET MARKUP CSV ON QUOTE OFF
--   SPOOL constraints.csv
--   @scripts/extract_constraints.sql
--   SPOOL OFF
--
-- Change the owner below to your target schema.

SELECT c.constraint_name    AS constraint_name,
       c.constraint_type    AS constraint_type,   -- P = primary, U = unique, R = foreign
       c.table_name         AS table_name,
       c.r_constraint_name  AS r_constraint_name,  -- for R rows: the referenced PK/UK constraint
       acc.column_name      AS column_name,
       acc.position         AS col_position
FROM   all_constraints  c
JOIN   all_cons_columns acc
       ON  acc.owner           = c.owner
       AND acc.constraint_name = c.constraint_name
WHERE  c.owner = 'CETIP'
  AND  c.constraint_type IN ('P', 'U', 'R')
ORDER  BY c.constraint_name, acc.position;
