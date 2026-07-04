-- Disable / relax the CETIP constraints that block the synthetic load.
--
-- Owner: CETIP. Run with a login that has ALTER TABLE on these tables.
--
-- IMPORTANT: an Oracle FOREIGN KEY permits NULLs — null FK values load fine with
-- the FK enabled. So a "nulls" blocker is a NOT NULL (mandatory) column
-- (ORA-01400), fixed by section 3 (MODIFY ... NULL), NOT by disabling the FK.
-- Only genuine orphan rows (non-null FK value with no parent) need the FK
-- disabled (section 2, ORA-02291).
--
-- Columns in scope:
--   LANCAMENTO.NUM_ID_ENTIDADE              -- 2 orphan rows (-> USUARIO)  : FK
--   OPERACAO.NUM_CONTA_PARTICIPANTE_P1      -- 8k nulls                    : NOT NULL?
--   OPERACAO.NUM_CONTA_PARTICIPANTE_P2      -- 3k nulls                    : NOT NULL?
--   ESPECIFICACAO_COMITENTE.NUM_ID_ENTIDADE -- 20k nulls (all)             : NOT NULL?
--   CONDICAO_IF.NUM_IF                      -- 15k nulls                   : NOT NULL?
-- =====================================================================


-- =====================================================================
-- 1. DIAGNOSE — is each column NULLABLE, and what is its FK constraint?
--    NULLABLE=Y + nulls   -> already loads, do nothing.
--    NULLABLE=N + nulls   -> NOT NULL blocker -> section 3.
--    orphans              -> FK blocker       -> section 2.
-- =====================================================================
SELECT tc.table_name, tc.column_name, tc.nullable,
       ac.constraint_name AS fk_name, ac.status AS fk_status
FROM   all_tab_columns tc
LEFT JOIN all_cons_columns acc
       ON acc.owner = tc.owner AND acc.table_name = tc.table_name
      AND acc.column_name = tc.column_name
LEFT JOIN all_constraints ac
       ON ac.owner = acc.owner AND ac.constraint_name = acc.constraint_name
      AND ac.constraint_type = 'R'
WHERE  tc.owner = 'CETIP'
  AND (tc.table_name, tc.column_name) IN (
      ('OPERACAO','NUM_CONTA_PARTICIPANTE_P1'),
      ('OPERACAO','NUM_CONTA_PARTICIPANTE_P2'),
      ('LANCAMENTO','NUM_ID_ENTIDADE'),
      ('ESPECIFICACAO_COMITENTE','NUM_ID_ENTIDADE'),
      ('CONDICAO_IF','NUM_IF'));


-- =====================================================================
-- 2. DISABLE the FK constraints (for genuine orphan-row blockers).
--    Resolves the constraint name per (table, column) automatically.
--    Trim the IN-list to only the columns the diagnostic flagged as FK.
-- =====================================================================
SET SERVEROUTPUT ON;
BEGIN
  FOR c IN (
    SELECT ac.owner, ac.table_name, ac.constraint_name
    FROM   all_constraints ac
    JOIN   all_cons_columns acc
      ON   ac.owner = acc.owner AND ac.constraint_name = acc.constraint_name
    WHERE  ac.constraint_type = 'R' AND ac.owner = 'CETIP'
      AND (ac.table_name, acc.column_name) IN (
        ('LANCAMENTO','NUM_ID_ENTIDADE'),
        ('OPERACAO','NUM_CONTA_PARTICIPANTE_P1'),
        ('OPERACAO','NUM_CONTA_PARTICIPANTE_P2'),
        ('ESPECIFICACAO_COMITENTE','NUM_ID_ENTIDADE'),
        ('CONDICAO_IF','NUM_IF'))
  ) LOOP
    EXECUTE IMMEDIATE 'ALTER TABLE "'||c.owner||'"."'||c.table_name||
                      '" DISABLE CONSTRAINT "'||c.constraint_name||'"';
    DBMS_OUTPUT.PUT_LINE('disabled '||c.table_name||'.'||c.constraint_name);
  END LOOP;
END;
/


-- =====================================================================
-- 3. MAKE mandatory columns NULLABLE (for NOT NULL blockers).
--    Metadata-only, instant. Only run for columns the diagnostic showed
--    as NULLABLE=N.
-- =====================================================================
ALTER TABLE CETIP.OPERACAO                MODIFY (NUM_CONTA_PARTICIPANTE_P1 NULL);
ALTER TABLE CETIP.OPERACAO                MODIFY (NUM_CONTA_PARTICIPANTE_P2 NULL);
ALTER TABLE CETIP.ESPECIFICACAO_COMITENTE MODIFY (NUM_ID_ENTIDADE NULL);
ALTER TABLE CETIP.CONDICAO_IF             MODIFY (NUM_IF NULL);


-- =====================================================================
-- POST-LOAD — re-protect (optional). Run after the load completes.
--   ENABLE VALIDATE would FAIL (ORA-02298) while the orphan/null rows
--   remain, so use NOVALIDATE to keep loaded rows and enforce new inserts.
--   Restoring NOT NULL (MODIFY ... NOT NULL) also fails while nulls remain.
-- =====================================================================
-- ALTER TABLE CETIP.LANCAMENTO ENABLE NOVALIDATE CONSTRAINT <fk_name_from_section_1>;


-- =====================================================================
-- BULK / SCHEMA-WIDE — disable EVERY constraint that can block ingestion.
--
-- Blockers for an INSERT: R (foreign key -> ORA-02291) and C (check,
-- incl. NOT NULL -> ORA-01400/02290). P/U (PK/unique) are left enabled by
-- default — synthetic PKs are unique by design and the load's pk-guard
-- protects against ORA-00001; see the OPTIONAL block to include them.
--
-- SCOPE: defaults to the whole CETIP schema. On a shared/production target
-- this removes integrity protection on ALL tables while disabled — prefer
-- scoping to the loaded tables (uncomment the AND table_name IN (...) line).
-- Only ENABLED constraints are touched; existing rows are never modified.
-- =====================================================================

-- 0. LIST what would be disabled (run this first, eyeball it):
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

-- 1. DISABLE all blocking (R + C) constraints:
SET SERVEROUTPUT ON;
BEGIN
  FOR c IN (
    SELECT owner, table_name, constraint_name, constraint_type
    FROM   all_constraints
    WHERE  owner = 'CETIP'
      AND  status = 'ENABLED'
      AND  constraint_type IN ('R','C')
--    AND table_name IN ( ... same list as above ... )
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
-- R must already be disabled (done above) so no enabled FK depends on them;
-- CASCADE is a backstop. Change 'R','C' above to add 'U','P', or run:
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

-- 2. POST-LOAD re-enable (NOVALIDATE keeps your loaded rows, enforces new inserts).
--    NOTE: this re-enables ALL currently-DISABLED R/C in CETIP — if some were
--    already disabled before you started, exclude them or accept they get enabled.
--    ENABLE VALIDATE would fail on the rows you just force-loaded; use NOVALIDATE.
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
