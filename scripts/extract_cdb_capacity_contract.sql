-- CDB capacity contract evidence for Oracle SQL*Plus, SQLcl, or SQL Developer.
--
-- This script is read-only. It reads only Oracle dictionary/metadata views and
-- does not scan business data in any of the target tables. The owner CETIP and
-- the target table names are hardcoded below. Each result set is returned to
-- the client for export by the user; this script does not redirect output to a file.
--
-- Result sets are intentionally separate so a privilege error in one view does
-- not prevent the client from receiving evidence from the other statements.

SET PAGESIZE 50000
SET LINESIZE 32767
SET LONG 1000000
SET LONGCHUNKSIZE 1000000
SET HEADING ON
SET FEEDBACK ON
SET VERIFY OFF
SET DEFINE OFF
SET TAB OFF

PROMPT === Result set 1A: database product component versions ===
SELECT product AS product,
       version AS version,
       status AS status
  FROM product_component_version
 ORDER BY product, version, status;

PROMPT === Result set 1B: database NLS parameters ===
SELECT parameter AS parameter,
       value AS value
  FROM nls_database_parameters
 WHERE parameter IN ('NLS_CHARACTERSET', 'NLS_NCHAR_CHARACTERSET')
 ORDER BY parameter;

PROMPT === Result set 2: CETIP table columns ===
WITH scope(table_name) AS (
    SELECT 'CARTEIRA_COMITENTE' FROM dual UNION ALL
    SELECT 'CARTEIRA_PARTICIPANTE' FROM dual UNION ALL
    SELECT 'CONDICAO_IF' FROM dual UNION ALL
    SELECT 'CREDITO' FROM dual UNION ALL
    SELECT 'DADO_OPERACAO' FROM dual UNION ALL
    SELECT 'DEPOSITO_AUTOMATICO_IF' FROM dual UNION ALL
    SELECT 'ESPECIFICACAO' FROM dual UNION ALL
    SELECT 'ESPECIFICACAO_COMITENTE' FROM dual UNION ALL
    SELECT 'EVENTO' FROM dual UNION ALL
    SELECT 'INSTRUMENTO_FINANCEIRO' FROM dual UNION ALL
    SELECT 'JUROS_FLUTUANTE' FROM dual UNION ALL
    SELECT 'LANCAMENTO' FROM dual UNION ALL
    SELECT 'OPERACAO' FROM dual UNION ALL
    SELECT 'RESGATE' FROM dual UNION ALL
    SELECT 'TITULO' FROM dual
)
SELECT c.owner AS owner,
       c.table_name AS table_name,
       c.column_id AS column_id,
       c.column_name AS column_name,
       c.data_type AS data_type,
       c.data_length AS data_length,
       c.char_length AS char_length,
       c.char_used AS char_used,
       c.data_precision AS data_precision,
       c.data_scale AS data_scale,
       c.nullable AS nullable
  FROM all_tab_columns c
  JOIN scope s
    ON s.table_name = c.table_name
 WHERE c.owner = 'CETIP'
 ORDER BY c.owner, c.table_name, c.column_id, c.column_name;

PROMPT === Result set 3: CETIP constraints and columns ===
WITH scope(table_name) AS (
    SELECT 'CARTEIRA_COMITENTE' FROM dual UNION ALL
    SELECT 'CARTEIRA_PARTICIPANTE' FROM dual UNION ALL
    SELECT 'CONDICAO_IF' FROM dual UNION ALL
    SELECT 'CREDITO' FROM dual UNION ALL
    SELECT 'DADO_OPERACAO' FROM dual UNION ALL
    SELECT 'DEPOSITO_AUTOMATICO_IF' FROM dual UNION ALL
    SELECT 'ESPECIFICACAO' FROM dual UNION ALL
    SELECT 'ESPECIFICACAO_COMITENTE' FROM dual UNION ALL
    SELECT 'EVENTO' FROM dual UNION ALL
    SELECT 'INSTRUMENTO_FINANCEIRO' FROM dual UNION ALL
    SELECT 'JUROS_FLUTUANTE' FROM dual UNION ALL
    SELECT 'LANCAMENTO' FROM dual UNION ALL
    SELECT 'OPERACAO' FROM dual UNION ALL
    SELECT 'RESGATE' FROM dual UNION ALL
    SELECT 'TITULO' FROM dual
)
SELECT ac.owner AS owner,
       ac.table_name AS table_name,
       ac.constraint_name AS constraint_name,
       cc.position AS position,
       cc.column_name AS column_name,
       ac.constraint_type AS constraint_type,
       ac.status AS status,
       ac.validated AS validated,
       ac.deferrable AS deferrable,
       ac.deferred AS deferred,
       ac.delete_rule AS delete_rule,
       ac.r_constraint_name AS r_constraint_name,
       rc.owner AS referenced_owner,
       rc.table_name AS referenced_table_name,
       rcc.column_name AS referenced_column_name
  FROM all_constraints ac
  JOIN scope s
    ON s.table_name = ac.table_name
  JOIN all_cons_columns cc
    ON cc.owner = ac.owner
   AND cc.constraint_name = ac.constraint_name
  LEFT JOIN all_constraints rc
    ON rc.owner = ac.r_owner
   AND rc.constraint_name = ac.r_constraint_name
  LEFT JOIN all_cons_columns rcc
    ON rcc.owner = rc.owner
   AND rcc.constraint_name = rc.constraint_name
   AND rcc.position = cc.position
 WHERE ac.owner = 'CETIP'
 ORDER BY ac.owner,
          ac.table_name,
          ac.constraint_name,
          cc.position,
          cc.column_name;

PROMPT === Result set 4: CETIP check constraints ===
WITH scope(table_name) AS (
    SELECT 'CARTEIRA_COMITENTE' FROM dual UNION ALL
    SELECT 'CARTEIRA_PARTICIPANTE' FROM dual UNION ALL
    SELECT 'CONDICAO_IF' FROM dual UNION ALL
    SELECT 'CREDITO' FROM dual UNION ALL
    SELECT 'DADO_OPERACAO' FROM dual UNION ALL
    SELECT 'DEPOSITO_AUTOMATICO_IF' FROM dual UNION ALL
    SELECT 'ESPECIFICACAO' FROM dual UNION ALL
    SELECT 'ESPECIFICACAO_COMITENTE' FROM dual UNION ALL
    SELECT 'EVENTO' FROM dual UNION ALL
    SELECT 'INSTRUMENTO_FINANCEIRO' FROM dual UNION ALL
    SELECT 'JUROS_FLUTUANTE' FROM dual UNION ALL
    SELECT 'LANCAMENTO' FROM dual UNION ALL
    SELECT 'OPERACAO' FROM dual UNION ALL
    SELECT 'RESGATE' FROM dual UNION ALL
    SELECT 'TITULO' FROM dual
)
SELECT ac.owner AS owner,
       ac.table_name AS table_name,
       ac.constraint_name AS constraint_name,
       ac.constraint_type AS constraint_type,
       ac.status AS status,
       ac.validated AS validated,
       ac.deferrable AS deferrable,
       ac.deferred AS deferred,
       ac.search_condition AS search_condition
  FROM all_constraints ac
  JOIN scope s
    ON s.table_name = ac.table_name
 WHERE ac.owner = 'CETIP'
   AND ac.constraint_type = 'C'
 ORDER BY ac.owner, ac.table_name, ac.constraint_name;

PROMPT === Result set 5: CETIP trigger inventory ===
WITH scope(table_name) AS (
    SELECT 'CARTEIRA_COMITENTE' FROM dual UNION ALL
    SELECT 'CARTEIRA_PARTICIPANTE' FROM dual UNION ALL
    SELECT 'CONDICAO_IF' FROM dual UNION ALL
    SELECT 'CREDITO' FROM dual UNION ALL
    SELECT 'DADO_OPERACAO' FROM dual UNION ALL
    SELECT 'DEPOSITO_AUTOMATICO_IF' FROM dual UNION ALL
    SELECT 'ESPECIFICACAO' FROM dual UNION ALL
    SELECT 'ESPECIFICACAO_COMITENTE' FROM dual UNION ALL
    SELECT 'EVENTO' FROM dual UNION ALL
    SELECT 'INSTRUMENTO_FINANCEIRO' FROM dual UNION ALL
    SELECT 'JUROS_FLUTUANTE' FROM dual UNION ALL
    SELECT 'LANCAMENTO' FROM dual UNION ALL
    SELECT 'OPERACAO' FROM dual UNION ALL
    SELECT 'RESGATE' FROM dual UNION ALL
    SELECT 'TITULO' FROM dual
)
SELECT t.owner AS owner,
       t.trigger_name AS trigger_name,
       t.trigger_type AS trigger_type,
       t.triggering_event AS triggering_event,
       t.table_owner AS table_owner,
       t.table_name AS table_name,
       t.status AS status,
       t.action_type AS action_type
  FROM all_triggers t
  JOIN scope s
    ON s.table_name = t.table_name
 WHERE t.table_owner = 'CETIP'
 ORDER BY t.owner, t.trigger_name, t.table_owner, t.table_name;

PROMPT === Result set 6: objects dependent on CETIP target tables ===
-- ALL_DEPENDENCIES records compile-time dependencies only; dynamic SQL does
-- not appear here and must be resolved from the failing call path separately.
WITH scope(table_name) AS (
    SELECT 'CARTEIRA_COMITENTE' FROM dual UNION ALL
    SELECT 'CARTEIRA_PARTICIPANTE' FROM dual UNION ALL
    SELECT 'CONDICAO_IF' FROM dual UNION ALL
    SELECT 'CREDITO' FROM dual UNION ALL
    SELECT 'DADO_OPERACAO' FROM dual UNION ALL
    SELECT 'DEPOSITO_AUTOMATICO_IF' FROM dual UNION ALL
    SELECT 'ESPECIFICACAO' FROM dual UNION ALL
    SELECT 'ESPECIFICACAO_COMITENTE' FROM dual UNION ALL
    SELECT 'EVENTO' FROM dual UNION ALL
    SELECT 'INSTRUMENTO_FINANCEIRO' FROM dual UNION ALL
    SELECT 'JUROS_FLUTUANTE' FROM dual UNION ALL
    SELECT 'LANCAMENTO' FROM dual UNION ALL
    SELECT 'OPERACAO' FROM dual UNION ALL
    SELECT 'RESGATE' FROM dual UNION ALL
    SELECT 'TITULO' FROM dual
)
SELECT d.owner AS owner,
       d.name AS name,
       d.type AS type,
       d.referenced_owner AS referenced_owner,
       d.referenced_name AS referenced_name,
       d.referenced_type AS referenced_type,
       d.dependency_type AS dependency_type
  FROM all_dependencies d
  JOIN scope s
    ON s.table_name = d.referenced_name
 WHERE d.referenced_owner = 'CETIP'
   AND d.referenced_type = 'TABLE'
   AND d.type IN ('PACKAGE', 'PACKAGE BODY', 'PROCEDURE', 'FUNCTION', 'TRIGGER')
 ORDER BY d.owner,
          d.type,
          d.name,
          d.referenced_owner,
          d.referenced_name,
          d.dependency_type;

PROMPT === Result set 7: arguments of routines dependent on CETIP target tables ===
WITH scope(table_name) AS (
    SELECT 'CARTEIRA_COMITENTE' FROM dual UNION ALL
    SELECT 'CARTEIRA_PARTICIPANTE' FROM dual UNION ALL
    SELECT 'CONDICAO_IF' FROM dual UNION ALL
    SELECT 'CREDITO' FROM dual UNION ALL
    SELECT 'DADO_OPERACAO' FROM dual UNION ALL
    SELECT 'DEPOSITO_AUTOMATICO_IF' FROM dual UNION ALL
    SELECT 'ESPECIFICACAO' FROM dual UNION ALL
    SELECT 'ESPECIFICACAO_COMITENTE' FROM dual UNION ALL
    SELECT 'EVENTO' FROM dual UNION ALL
    SELECT 'INSTRUMENTO_FINANCEIRO' FROM dual UNION ALL
    SELECT 'JUROS_FLUTUANTE' FROM dual UNION ALL
    SELECT 'LANCAMENTO' FROM dual UNION ALL
    SELECT 'OPERACAO' FROM dual UNION ALL
    SELECT 'RESGATE' FROM dual UNION ALL
    SELECT 'TITULO' FROM dual
),
dependent_routines AS (
    SELECT d.owner AS owner,
           CASE
               WHEN d.type IN ('PACKAGE', 'PACKAGE BODY') THEN 'PACKAGE'
               ELSE d.type
           END AS logical_type,
           d.name AS object_name
      FROM all_dependencies d
      JOIN scope s
        ON s.table_name = d.referenced_name
     WHERE d.referenced_owner = 'CETIP'
       AND d.referenced_type = 'TABLE'
       AND d.type IN ('PACKAGE', 'PACKAGE BODY', 'PROCEDURE', 'FUNCTION')
     GROUP BY d.owner,
              CASE
                  WHEN d.type IN ('PACKAGE', 'PACKAGE BODY') THEN 'PACKAGE'
                  ELSE d.type
              END,
              d.name
)
SELECT a.owner AS owner,
       a.package_name AS package_name,
       a.object_name AS object_name,
       a.overload AS overload,
       a.subprogram_id AS subprogram_id,
       a.argument_name AS argument_name,
       a.position AS position,
       a.sequence AS sequence,
       a.in_out AS in_out,
       a.data_type AS data_type,
       a.data_length AS data_length,
       a.char_length AS char_length,
       a.char_used AS char_used,
       a.data_precision AS data_precision,
       a.data_scale AS data_scale,
       a.type_owner AS type_owner,
       a.type_name AS type_name,
       a.type_subname AS type_subname
  FROM all_arguments a
  JOIN dependent_routines r
    ON r.owner = a.owner
   AND (
          (r.logical_type = 'PACKAGE' AND a.package_name = r.object_name)
       OR (r.logical_type IN ('PROCEDURE', 'FUNCTION')
           AND a.package_name IS NULL
           AND a.object_name = r.object_name)
       )
 ORDER BY a.owner,
          a.package_name,
          a.object_name,
          a.overload,
          a.subprogram_id,
          a.sequence,
          a.position,
          a.argument_name;
