-- Disable every CETIP constraint that can block the synthetic ingestion.
--
-- Owner: CETIP. Run with a login that has ALTER TABLE (or ALTER ANY TABLE).
-- Workflow:  0) LIST  ->  1) DISABLE  ->  load with --skip-validation  ->  2) RE-ENABLE.
--
-- Blockers for an INSERT are R (foreign key -> ORA-02291) and C (check,
-- incl. NOT NULL -> ORA-01400 / ORA-02290). P/U (primary/unique) are left
-- ENABLED by default: synthetic PKs are unique by design and load_tables' pk
-- dup-guard prevents ORA-00001, and disabling a PK drops its index. See the
-- OPTIONAL block below to include them.
--
-- SCOPE: defaults to the WHOLE CETIP schema. On a shared/production target this
-- removes integrity protection from every table while disabled — prefer scoping
-- to the loaded tables (uncomment the `AND table_name IN (...)` lines). Only
-- ENABLED constraints are touched; existing rows are never modified.
-- =====================================================================


-- 0. LIST what would be disabled (run first, eyeball it):
SELECT table_name, constraint_type, constraint_name, status
FROM   all_constraints
WHERE  owner = 'CETIP'
  AND  status = 'ENABLED'
  AND  constraint_type IN ('R','C')
--  AND table_name IN ('INSTRUMENTO_FINANCEIRO','OPERACAO','EVENTO','CONDICAO_IF',
--                     'LANCAMENTO','ESPECIFICACAO','ESPECIFICACAO_COMITENTE',
--                     'DADO_OPERACAO','CREDITO','RESGATE','TITULO','JUROS_FLUTUANTE',
--                     'CARTEIRA_COMITENTE','CARTEIRA_PARTICIPANTE','DEPOSITO_AUTOMATICO_IF')
ORDER BY table_name, constraint_type;


-- 1. DISABLE all blocking (R + C) constraints. Each ALTER is wrapped so one
--    failure (e.g. a dependency) logs SKIP and the sweep continues:
SET SERVEROUTPUT ON;
BEGIN
  FOR c IN (
    SELECT owner, table_name, constraint_name, constraint_type
    FROM   all_constraints
    WHERE  owner = 'CETIP'
      AND  status = 'ENABLED'
      AND  constraint_type IN ('R','C')
--    AND table_name IN ( ... same list as section 0 ... )
  ) LOOP
    BEGIN
      EXECUTE IMMEDIATE 'ALTER TABLE "'||c.owner||'"."'||c.table_name||
                        '" DISABLE CONSTRAINT "'||c.constraint_name||'"';
      DBMS_OUTPUT.PUT_LINE('disabled '||c.constraint_type||' '||
                           c.table_name||'.'||c.constraint_name);
    EXCEPTION WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('SKIP '||c.table_name||'.'||c.constraint_name||' : '||SQLERRM);
    END;
  END LOOP;
END;
/


-- OPTIONAL — also disable UNIQUE (U) and PRIMARY KEY (P). Drops their indexes
-- and removes ORA-00001 protection; only if the synthetic data may violate them.
-- Run AFTER section 1 (R disabled) so no enabled FK depends on them; CASCADE is
-- a backstop.
-- BEGIN
--   FOR c IN (SELECT owner, table_name, constraint_name FROM all_constraints
--             WHERE owner='CETIP' AND status='ENABLED' AND constraint_type IN ('U','P')) LOOP
--     BEGIN
--       EXECUTE IMMEDIATE 'ALTER TABLE "'||c.owner||'"."'||c.table_name||
--                         '" DISABLE CONSTRAINT "'||c.constraint_name||'" CASCADE';
--     EXCEPTION WHEN OTHERS THEN DBMS_OUTPUT.PUT_LINE('SKIP '||c.constraint_name||' '||SQLERRM);
--     END;
--   END LOOP;
-- END;
-- /


-- 2. POST-LOAD re-enable (run after the load). NOVALIDATE keeps the rows you
--    just force-loaded and enforces future inserts; ENABLE VALIDATE would fail
--    (ORA-02298) on those rows. NOTE: this re-enables every currently-DISABLED
--    R/C in CETIP — if some were already disabled before you started, exclude
--    them or accept that they get enabled.
BEGIN
  FOR c IN (
    SELECT owner, table_name, constraint_name
    FROM   all_constraints
    WHERE  owner = 'CETIP'
      AND  status = 'DISABLED'
      AND  constraint_type IN ('R','C')
  ) LOOP
    BEGIN
      EXECUTE IMMEDIATE 'ALTER TABLE "'||c.owner||'"."'||c.table_name||
                        '" ENABLE NOVALIDATE CONSTRAINT "'||c.constraint_name||'"';
    EXCEPTION WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('SKIP '||c.table_name||'.'||c.constraint_name||' : '||SQLERRM);
    END;
  END LOOP;
END;
/
