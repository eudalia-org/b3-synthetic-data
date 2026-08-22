-Valida lastro e lci n tem dependencia
SELECT COUNT(DISTINCT SCR.NUM_IF) FROM CETIP.CREDITO_SCR SCR
  JOIN CETIP.INSTRUMENTO_FINANCEIRO I ON I.NUM_IF = SCR.NUM_IF
 WHERE I.NUM_TIPO_IF = 81 AND SCR.DAT_EXCLUSAO IS NULL AND I.DAT_EXCLUSAO IS NULL;  -- LCI+lastro

SELECT COUNT(DISTINCT CDC.NUM_IF) FROM CETIP.CREDITO_DC CDC
  JOIN CETIP.INSTRUMENTO_FINANCEIRO I ON I.NUM_IF = CDC.NUM_IF
 WHERE I.NUM_TIPO_IF = 96 AND CDC.DAT_EXCLUSAO IS NULL AND I.DAT_EXCLUSAO IS NULL;  -- LCA+DC

SELECT NUM_TIPO_IF, COUNT(*) FROM CETIP.INSTRUMENTO_FINANCEIRO
 WHERE NUM_TIPO_IF IN (81,96) AND DAT_EXCLUSAO IS NULL GROUP BY NUM_TIPO_IF;        -- sanidade


-tenta ver dependencia
  SELECT SCR.NUM_TIPO_IF, COUNT(*) linhas, COUNT(DISTINCT SCR.NUM_IF) instrumentos
FROM CETIP.CREDITO_SCR SCR WHERE SCR.DAT_EXCLUSAO IS NULL
GROUP BY SCR.NUM_TIPO_IF ORDER BY 2 DESC;

SELECT CDC.NUM_TIPO_IF, COUNT(*) linhas, COUNT(DISTINCT CDC.NUM_IF) instrumentos
FROM CETIP.CREDITO_DC CDC WHERE CDC.DAT_EXCLUSAO IS NULL
GROUP BY CDC.NUM_TIPO_IF ORDER BY 2 DESC;



-- 1) A tabela tem coluna NUM_IF? (o spec só declara NUM_IF_PERTENCE)
SELECT column_name, data_type, nullable FROM all_tab_columns
WHERE owner='CETIP' AND table_name='HISTORICO_IF_TITULO' AND column_name LIKE '%NUM_IF%'
ORDER BY column_name;

-- 2) Que FK o Oracle REALMENTE declara para INSTRUMENTO_FINANCEIRO?
SELECT acc.column_name, accr.column_name AS coluna_pai
FROM all_constraints ac
JOIN all_cons_columns acc ON acc.owner=ac.owner AND acc.constraint_name=ac.constraint_name
JOIN all_constraints acr ON acr.owner=ac.r_owner AND acr.constraint_name=ac.r_constraint_name
JOIN all_cons_columns accr ON accr.owner=acr.owner AND accr.constraint_name=acr.constraint_name
                          AND accr.position=acc.position
WHERE ac.owner='CETIP' AND ac.table_name='HISTORICO_IF_TITULO'
  AND ac.constraint_type='R' AND acr.table_name='INSTRUMENTO_FINANCEIRO';

-- 3) NUM_IF_PERTENCE é usável? (é nullable no spec)
SELECT COUNT(*) total, COUNT(NUM_IF_PERTENCE) preenchidos FROM CETIP.HISTORICO_IF_TITULO;

-- 4) Quanto se perde: CCB tem histórico de título? (rode só se (1) devolver NUM_IF)
SELECT COUNT(*) FROM CETIP.HISTORICO_IF_TITULO H
JOIN CETIP.INSTRUMENTO_FINANCEIRO I ON I.NUM_IF = H.NUM_IF
WHERE I.NUM_TIPO_IF = 53 AND I.DAT_EXCLUSAO IS NULL;
