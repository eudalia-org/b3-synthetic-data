-- CDB simplificado: strict, attributable NUM_IF projection over synthetic Parquet.
--
-- Run locally with Spark SQL (environment substitution is enabled by default):
--   DATAGEN_SYNTHETIC_BASE_URI='file:///absolute/path/to/output' \
--     spark-sql --conf spark.sql.variable.substitute=true \
--     -f scripts/select_valid_cdb_num_ifs.sql
--
-- OCI Data Flow SQL runner: set DATAGEN_SYNTHETIC_BASE_URI to the base oci:// URI,
-- enable spark.sql.variable.substitute, and execute this file with the OCI Hadoop
-- connector/configuration that makes oci:// Parquet paths readable.
--
-- IMPORTANT: THIS QUERY DOES NOT CLAIM FULL PARITY WITH validate_cdb_simplificado.py.
-- It intentionally omits all checks that require unavailable views, Oracle metadata,
-- a production connection, a baseline, or application code:
--   * V_FAMILIA_CONTAS area/access checks;
--   * V_OBJETOS_SERVICO platform checks;
--   * V_PARAMETRO_SIC CDB compatibility checks;
--   * Oracle metadata-driven PK, FK, NOT NULL, capacity, and NLS checks;
--   * FK checks against target/Oracle parent rows;
--   * shape-baseline unseen-shape and distribution-drift checks;
--   * production verification of the subtype map;
--   * the application-capacity contract;
--   * --registration-profile formats, constants, and persisted type-mix checks.
-- Only JUROS_FLUTUANTE (type 3) and RESGATE (type 20) subtype membership is
-- checked. Other subtype tables are deliberately not assumed to exist. A subtype
-- row with no CONDICAO_IF parent has no NUM_IF and therefore cannot be attributed
-- by these available columns; such globally orphaned keys are not projected onto
-- arbitrary IDs. Likewise, a violating row with no path to NUM_IF cannot exclude
-- an arbitrary ID.
--
-- WARN-level checks that are expressible from the approved physical inputs (notably
-- sem-modalidade) exclude the attributable NUM_IF. The baseline-free shape rule is
-- deliberately stricter than the validator's dataset-level percentage tolerance:
-- every attributable violating ID is excluded (zero tolerance).
--
-- Empty approved lookup datasets invalidate only NUM_IFs that carry actual non-null,
-- nonblank account/TOS references; IDs with no references or operations can pass.
-- Spark SQL cannot turn a missing path or missing required column into an empty result
-- because resolution fails before the query runs; those cases intentionally fail
-- closed with an analysis/read error.

CREATE OR REPLACE TEMP VIEW src_instrumento_financeiro
USING parquet
OPTIONS (path '${env:DATAGEN_SYNTHETIC_BASE_URI}/INSTRUMENTO_FINANCEIRO');

CREATE OR REPLACE TEMP VIEW src_titulo
USING parquet
OPTIONS (path '${env:DATAGEN_SYNTHETIC_BASE_URI}/TITULO');

CREATE OR REPLACE TEMP VIEW src_deposito_automatico_if
USING parquet
OPTIONS (path '${env:DATAGEN_SYNTHETIC_BASE_URI}/DEPOSITO_AUTOMATICO_IF');

CREATE OR REPLACE TEMP VIEW src_condicao_if
USING parquet
OPTIONS (path '${env:DATAGEN_SYNTHETIC_BASE_URI}/CONDICAO_IF');

CREATE OR REPLACE TEMP VIEW src_juros_flutuante
USING parquet
OPTIONS (path '${env:DATAGEN_SYNTHETIC_BASE_URI}/JUROS_FLUTUANTE');

CREATE OR REPLACE TEMP VIEW src_resgate
USING parquet
OPTIONS (path '${env:DATAGEN_SYNTHETIC_BASE_URI}/RESGATE');

CREATE OR REPLACE TEMP VIEW src_operacao
USING parquet
OPTIONS (path '${env:DATAGEN_SYNTHETIC_BASE_URI}/OPERACAO');

CREATE OR REPLACE TEMP VIEW src_dado_operacao
USING parquet
OPTIONS (path '${env:DATAGEN_SYNTHETIC_BASE_URI}/DADO_OPERACAO');

CREATE OR REPLACE TEMP VIEW src_lancamento
USING parquet
OPTIONS (path '${env:DATAGEN_SYNTHETIC_BASE_URI}/LANCAMENTO');

CREATE OR REPLACE TEMP VIEW src_conta_participante
USING parquet
OPTIONS (path '${env:DATAGEN_SYNTHETIC_BASE_URI}/CONTA_PARTICIPANTE');

CREATE OR REPLACE TEMP VIEW src_tipo_oper_objeto_serv
USING parquet
OPTIONS (path '${env:DATAGEN_SYNTHETIC_BASE_URI}/TIPO_OPER_OBJETO_SERV');

CREATE OR REPLACE TEMP VIEW src_tipo_operacao
USING parquet
OPTIONS (path '${env:DATAGEN_SYNTHETIC_BASE_URI}/TIPO_OPERACAO');

WITH
root_rows AS (
  SELECT
    CASE
      WHEN TRIM(CAST(NUM_IF AS STRING)) RLIKE '^-?[0-9]+[.][0-9]*0$'
        THEN REGEXP_REPLACE(
          REGEXP_REPLACE(TRIM(CAST(NUM_IF AS STRING)), '([.][0-9]*?)0+$', '$1'),
          '[.]$',
          ''
        )
      ELSE TRIM(CAST(NUM_IF AS STRING))
    END AS NUM_IF,
    REGEXP_REPLACE(TRIM(CAST(NUM_TIPO_IF AS STRING)), '[.]0$', '') AS NUM_TIPO_IF,
    DAT_EXCLUSAO,
    DAT_EMISSAO,
    DAT_REGISTRO,
    DAT_VENCIMENTO,
    COD_IF,
    REGEXP_REPLACE(TRIM(CAST(COD_IF AS STRING)), '[.]0$', '') AS normalized_cod_if
  FROM src_instrumento_financeiro
),
candidates AS (
  SELECT DISTINCT NUM_IF
  FROM root_rows
  WHERE NUM_TIPO_IF = '49'
    AND DAT_EXCLUSAO IS NULL
    AND NUM_IF IS NOT NULL
    AND NUM_IF <> ''
),
title_rows AS (
  SELECT
    CASE
      WHEN TRIM(CAST(NUM_IF AS STRING)) RLIKE '^-?[0-9]+[.][0-9]*0$'
        THEN REGEXP_REPLACE(
          REGEXP_REPLACE(TRIM(CAST(NUM_IF AS STRING)), '([.][0-9]*?)0+$', '$1'),
          '[.]$',
          ''
        )
      ELSE TRIM(CAST(NUM_IF AS STRING))
    END AS NUM_IF,
    COD_TIPO_ESCALONAMENTO,
    DAT_EMISSAO,
    DAT_VENCIMENTO,
    NUM_CONTA_PARTICIPANTE
  FROM src_titulo
),
deposit_rows AS (
  SELECT
    CASE
      WHEN TRIM(CAST(NUM_IF AS STRING)) RLIKE '^-?[0-9]+[.][0-9]*0$'
        THEN REGEXP_REPLACE(
          REGEXP_REPLACE(TRIM(CAST(NUM_IF AS STRING)), '([.][0-9]*?)0+$', '$1'),
          '[.]$',
          ''
        )
      ELSE TRIM(CAST(NUM_IF AS STRING))
    END AS NUM_IF,
    NUM_CONTA_PARTICIPANTE
  FROM src_deposito_automatico_if
),
condition_rows AS (
  SELECT
    CASE
      WHEN TRIM(CAST(NUM_CONDICAO_IF AS STRING)) RLIKE '^-?[0-9]+[.][0-9]*0$'
        THEN REGEXP_REPLACE(
          REGEXP_REPLACE(
            TRIM(CAST(NUM_CONDICAO_IF AS STRING)),
            '([.][0-9]*?)0+$',
            '$1'
          ),
          '[.]$',
          ''
        )
      ELSE TRIM(CAST(NUM_CONDICAO_IF AS STRING))
    END AS NUM_CONDICAO_IF,
    CASE
      WHEN TRIM(CAST(NUM_IF AS STRING)) RLIKE '^-?[0-9]+[.][0-9]*0$'
        THEN REGEXP_REPLACE(
          REGEXP_REPLACE(TRIM(CAST(NUM_IF AS STRING)), '([.][0-9]*?)0+$', '$1'),
          '[.]$',
          ''
        )
      ELSE TRIM(CAST(NUM_IF AS STRING))
    END AS NUM_IF,
    REGEXP_REPLACE(
      TRIM(CAST(COD_TIPO_CONDICAO_IF AS STRING)),
      '[.]0$',
      ''
    ) AS COD_TIPO_CONDICAO_IF,
    DAT_EXCLUSAO,
    DAT_INICIO_CONDICAO_IF,
    DAT_FIM_CONDICAO_IF
  FROM src_condicao_if
),
floating_rows AS (
  SELECT
    CASE
      WHEN TRIM(CAST(NUM_CONDICAO_IF AS STRING)) RLIKE '^-?[0-9]+[.][0-9]*0$'
        THEN REGEXP_REPLACE(
          REGEXP_REPLACE(
            TRIM(CAST(NUM_CONDICAO_IF AS STRING)),
            '([.][0-9]*?)0+$',
            '$1'
          ),
          '[.]$',
          ''
        )
      ELSE TRIM(CAST(NUM_CONDICAO_IF AS STRING))
    END AS NUM_CONDICAO_IF
  FROM src_juros_flutuante
),
resgate_rows AS (
  SELECT
    CASE
      WHEN TRIM(CAST(NUM_CONDICAO_IF AS STRING)) RLIKE '^-?[0-9]+[.][0-9]*0$'
        THEN REGEXP_REPLACE(
          REGEXP_REPLACE(
            TRIM(CAST(NUM_CONDICAO_IF AS STRING)),
            '([.][0-9]*?)0+$',
            '$1'
          ),
          '[.]$',
          ''
        )
      ELSE TRIM(CAST(NUM_CONDICAO_IF AS STRING))
    END AS NUM_CONDICAO_IF,
    COD_COND_RESGATE,
    DAT_EXCLUSAO
  FROM src_resgate
),
operation_rows AS (
  SELECT
    CASE
      WHEN TRIM(CAST(NUM_ID_OPERACAO AS STRING)) RLIKE '^-?[0-9]+[.][0-9]*0$'
        THEN REGEXP_REPLACE(
          REGEXP_REPLACE(
            TRIM(CAST(NUM_ID_OPERACAO AS STRING)),
            '([.][0-9]*?)0+$',
            '$1'
          ),
          '[.]$',
          ''
        )
      ELSE TRIM(CAST(NUM_ID_OPERACAO AS STRING))
    END AS NUM_ID_OPERACAO,
    CASE
      WHEN TRIM(CAST(NUM_IF AS STRING)) RLIKE '^-?[0-9]+[.][0-9]*0$'
        THEN REGEXP_REPLACE(
          REGEXP_REPLACE(TRIM(CAST(NUM_IF AS STRING)), '([.][0-9]*?)0+$', '$1'),
          '[.]$',
          ''
        )
      ELSE TRIM(CAST(NUM_IF AS STRING))
    END AS NUM_IF,
    DAT_EXCLUSAO,
    COD_OPERACAO,
    REGEXP_REPLACE(TRIM(CAST(COD_OPERACAO AS STRING)), '[.]0$', '')
      AS normalized_cod_operacao,
    DAT_OPERACAO,
    NUM_CONTA_PARTICIPANTE_P1,
    NUM_CONTA_PARTICIPANTE_P2,
    NUM_CONTROLE_LANCAMENTO_P1,
    NUM_CONTROLE_LANCAMENTO_P2,
    NUM_ID_TIPO_OPER_OBJETO_SERV,
    NUM_ID_MODALIDADE_LIQUIDACAO
  FROM src_operacao
),
active_operations AS (
  SELECT *
  FROM operation_rows
  WHERE DAT_EXCLUSAO IS NULL
),
dado_rows AS (
  SELECT
    CASE
      WHEN TRIM(CAST(NUM_ID_OPERACAO AS STRING)) RLIKE '^-?[0-9]+[.][0-9]*0$'
        THEN REGEXP_REPLACE(
          REGEXP_REPLACE(
            TRIM(CAST(NUM_ID_OPERACAO AS STRING)),
            '([.][0-9]*?)0+$',
            '$1'
          ),
          '[.]$',
          ''
        )
      ELSE TRIM(CAST(NUM_ID_OPERACAO AS STRING))
    END AS NUM_ID_OPERACAO
  FROM src_dado_operacao
),
lancamento_rows AS (
  SELECT
    CASE
      WHEN TRIM(CAST(NUM_ID_OPERACAO AS STRING)) RLIKE '^-?[0-9]+[.][0-9]*0$'
        THEN REGEXP_REPLACE(
          REGEXP_REPLACE(
            TRIM(CAST(NUM_ID_OPERACAO AS STRING)),
            '([.][0-9]*?)0+$',
            '$1'
          ),
          '[.]$',
          ''
        )
      ELSE TRIM(CAST(NUM_ID_OPERACAO AS STRING))
    END AS NUM_ID_OPERACAO
  FROM src_lancamento
),
account_rows AS (
  SELECT
    CASE
      WHEN TRIM(CAST(NUM_CONTA_PARTICIPANTE AS STRING)) RLIKE '^-?[0-9]+[.][0-9]*0$'
        THEN REGEXP_REPLACE(
          REGEXP_REPLACE(
            TRIM(CAST(NUM_CONTA_PARTICIPANTE AS STRING)),
            '([.][0-9]*?)0+$',
            '$1'
          ),
          '[.]$',
          ''
        )
      ELSE TRIM(CAST(NUM_CONTA_PARTICIPANTE AS STRING))
    END AS account_id,
    REGEXP_REPLACE(
      TRIM(CAST(NUM_ID_SITUACAO_CONTA AS STRING)),
      '[.]0$',
      ''
    ) AS situation_id,
    TRIM(CAST(COD_CONTA_PARTICIPANTE AS STRING)) AS account_code
  FROM src_conta_participante
),
eligible_accounts AS (
  SELECT DISTINCT account_id
  FROM account_rows
  WHERE account_id IS NOT NULL
    AND account_id <> ''
    AND situation_id = '1'
    AND account_code RLIKE '^[0-9]{5}[.](40|10)-[0-9]$'
),
tos_rows AS (
  SELECT
    CASE
      WHEN TRIM(CAST(NUM_ID_TIPO_OPER_OBJETO_SERV AS STRING))
        RLIKE '^-?[0-9]+[.][0-9]*0$'
        THEN REGEXP_REPLACE(
          REGEXP_REPLACE(
            TRIM(CAST(NUM_ID_TIPO_OPER_OBJETO_SERV AS STRING)),
            '([.][0-9]*?)0+$',
            '$1'
          ),
          '[.]$',
          ''
        )
      ELSE TRIM(CAST(NUM_ID_TIPO_OPER_OBJETO_SERV AS STRING))
    END AS tos_id,
    CASE
      WHEN TRIM(CAST(NUM_ID_TIPO_OPERACAO AS STRING)) RLIKE '^-?[0-9]+[.][0-9]*0$'
        THEN REGEXP_REPLACE(
          REGEXP_REPLACE(
            TRIM(CAST(NUM_ID_TIPO_OPERACAO AS STRING)),
            '([.][0-9]*?)0+$',
            '$1'
          ),
          '[.]$',
          ''
        )
      ELSE TRIM(CAST(NUM_ID_TIPO_OPERACAO AS STRING))
    END AS tipo_operacao_id,
    REGEXP_REPLACE(
      TRIM(CAST(NUM_ID_OBJETO_SERVICO AS STRING)),
      '[.]0$',
      ''
    ) AS objeto_servico_id,
    TRIM(CAST(IND_DISPONIVEL_IDENTIFICACAO AS STRING)) AS identification_flag
  FROM src_tipo_oper_objeto_serv
),
tipo_operacao_rows AS (
  SELECT
    CASE
      WHEN TRIM(CAST(NUM_ID_TIPO_OPERACAO AS STRING)) RLIKE '^-?[0-9]+[.][0-9]*0$'
        THEN REGEXP_REPLACE(
          REGEXP_REPLACE(
            TRIM(CAST(NUM_ID_TIPO_OPERACAO AS STRING)),
            '([.][0-9]*?)0+$',
            '$1'
          ),
          '[.]$',
          ''
        )
      ELSE TRIM(CAST(NUM_ID_TIPO_OPERACAO AS STRING))
    END AS tipo_operacao_id,
    CAST(COD_TIPO_OPERACAO AS STRING) AS operation_type_code,
    TRIM(CAST(IND_SEM_MODALIDADE_INFOHUB AS STRING)) AS sem_modalidade_flag
  FROM src_tipo_operacao
),
valid_tos AS (
  SELECT DISTINCT t.tos_id
  FROM tos_rows t
  INNER JOIN tipo_operacao_rows o
    ON o.tipo_operacao_id = t.tipo_operacao_id
  WHERE t.tos_id IS NOT NULL
    AND t.tos_id <> ''
    AND t.objeto_servico_id = '44'
    AND t.identification_flag = 'S'
    AND o.operation_type_code = '1'
),
valid_sem_modalidade_tos AS (
  SELECT DISTINCT t.tos_id
  FROM tos_rows t
  INNER JOIN tipo_operacao_rows o
    ON o.tipo_operacao_id = t.tipo_operacao_id
  WHERE o.sem_modalidade_flag = 'S'
),
root_domain_invalid AS (
  SELECT DISTINCT r.NUM_IF, 'domain.instrumento_financeiro' AS reason
  FROM root_rows r
  INNER JOIN candidates c ON c.NUM_IF = r.NUM_IF
  WHERE (r.NUM_TIPO_IF IS NOT NULL AND r.NUM_TIPO_IF <> '49')
     OR r.DAT_EXCLUSAO IS NOT NULL
),
root_date_invalid AS (
  SELECT DISTINCT r.NUM_IF, 'date.instrumento_emissao_vencimento' AS reason
  FROM root_rows r
  INNER JOIN candidates c ON c.NUM_IF = r.NUM_IF
  WHERE TO_DATE(r.DAT_EMISSAO) IS NOT NULL
    AND TO_DATE(r.DAT_VENCIMENTO) IS NOT NULL
    AND TO_DATE(r.DAT_EMISSAO) > TO_DATE(r.DAT_VENCIMENTO)
  UNION ALL
  SELECT DISTINCT r.NUM_IF, 'date.instrumento_registro_vencimento' AS reason
  FROM root_rows r
  INNER JOIN candidates c ON c.NUM_IF = r.NUM_IF
  WHERE TO_DATE(r.DAT_REGISTRO) IS NOT NULL
    AND TO_DATE(r.DAT_VENCIMENTO) IS NOT NULL
    AND TO_DATE(r.DAT_REGISTRO) > TO_DATE(r.DAT_VENCIMENTO)
),
title_invalid AS (
  SELECT DISTINCT t.NUM_IF, 'domain.titulo_escalonado' AS reason
  FROM title_rows t
  INNER JOIN candidates c ON c.NUM_IF = t.NUM_IF
  WHERE t.COD_TIPO_ESCALONAMENTO IS NOT NULL
  UNION ALL
  SELECT DISTINCT t.NUM_IF, 'date.titulo_emissao_vencimento' AS reason
  FROM title_rows t
  INNER JOIN candidates c ON c.NUM_IF = t.NUM_IF
  WHERE TO_DATE(t.DAT_EMISSAO) IS NOT NULL
    AND TO_DATE(t.DAT_VENCIMENTO) IS NOT NULL
    AND TO_DATE(t.DAT_EMISSAO) > TO_DATE(t.DAT_VENCIMENTO)
),
condition_invalid AS (
  SELECT DISTINCT ci.NUM_IF, 'domain.condicao_excluida' AS reason
  FROM condition_rows ci
  INNER JOIN candidates c ON c.NUM_IF = ci.NUM_IF
  WHERE ci.DAT_EXCLUSAO IS NOT NULL
  UNION ALL
  SELECT DISTINCT ci.NUM_IF, 'date.condicao_inicio_fim' AS reason
  FROM condition_rows ci
  INNER JOIN candidates c ON c.NUM_IF = ci.NUM_IF
  WHERE TO_DATE(ci.DAT_INICIO_CONDICAO_IF) IS NOT NULL
    AND TO_DATE(ci.DAT_FIM_CONDICAO_IF) IS NOT NULL
    AND TO_DATE(ci.DAT_INICIO_CONDICAO_IF) > TO_DATE(ci.DAT_FIM_CONDICAO_IF)
),
resgate_domain_invalid AS (
  SELECT DISTINCT ci.NUM_IF, 'domain.resgate' AS reason
  FROM resgate_rows r
  INNER JOIN condition_rows ci
    ON ci.NUM_CONDICAO_IF = r.NUM_CONDICAO_IF
  INNER JOIN candidates c ON c.NUM_IF = ci.NUM_IF
  WHERE r.DAT_EXCLUSAO IS NOT NULL
     OR UPPER(TRIM(CAST(r.COD_COND_RESGATE AS STRING))) <> 'SEM TABELA'
     OR r.COD_COND_RESGATE IS NULL
),
subtype_membership AS (
  SELECT
    k.NUM_CONDICAO_IF,
    SUM(CASE WHEN k.subtype_name = 'JUROS_FLUTUANTE' THEN 1 ELSE 0 END) AS floating_rows,
    SUM(CASE WHEN k.subtype_name = 'RESGATE' THEN 1 ELSE 0 END) AS resgate_rows
  FROM (
    SELECT NUM_CONDICAO_IF, 'JUROS_FLUTUANTE' AS subtype_name
    FROM floating_rows
    UNION ALL
    SELECT NUM_CONDICAO_IF, 'RESGATE' AS subtype_name
    FROM resgate_rows
  ) k
  GROUP BY k.NUM_CONDICAO_IF
),
polymorphism_invalid AS (
  SELECT DISTINCT ci.NUM_IF, 'polymorphism.available_subtypes' AS reason
  FROM condition_rows ci
  INNER JOIN candidates c ON c.NUM_IF = ci.NUM_IF
  LEFT JOIN subtype_membership m
    ON m.NUM_CONDICAO_IF = ci.NUM_CONDICAO_IF
  WHERE ci.DAT_EXCLUSAO IS NULL
    AND NOT (
      (
        ci.COD_TIPO_CONDICAO_IF = '3'
        AND COALESCE(m.floating_rows, 0) = 1
        AND COALESCE(m.resgate_rows, 0) = 0
      )
      OR (
        ci.COD_TIPO_CONDICAO_IF = '20'
        AND COALESCE(m.floating_rows, 0) = 0
        AND COALESCE(m.resgate_rows, 0) = 1
      )
      OR (
        ci.COD_TIPO_CONDICAO_IF IS NOT NULL
        AND ci.COD_TIPO_CONDICAO_IF <> '3'
        AND ci.COD_TIPO_CONDICAO_IF <> '20'
        AND COALESCE(m.floating_rows, 0) = 0
        AND COALESCE(m.resgate_rows, 0) = 0
      )
    )
),
unknown_condition_type_invalid AS (
  SELECT DISTINCT ci.NUM_IF, 'polymorphism.unknown_or_null_type' AS reason
  FROM condition_rows ci
  INNER JOIN candidates c ON c.NUM_IF = ci.NUM_IF
  WHERE ci.DAT_EXCLUSAO IS NULL
    AND (
      ci.COD_TIPO_CONDICAO_IF IS NULL
      OR NOT (
        ci.COD_TIPO_CONDICAO_IF = '1'
        OR ci.COD_TIPO_CONDICAO_IF = '2'
        OR ci.COD_TIPO_CONDICAO_IF = '3'
        OR ci.COD_TIPO_CONDICAO_IF = '4'
        OR ci.COD_TIPO_CONDICAO_IF = '5'
        OR ci.COD_TIPO_CONDICAO_IF = '6'
        OR ci.COD_TIPO_CONDICAO_IF = '7'
        OR ci.COD_TIPO_CONDICAO_IF = '14'
        OR ci.COD_TIPO_CONDICAO_IF = '15'
        OR ci.COD_TIPO_CONDICAO_IF = '16'
        OR ci.COD_TIPO_CONDICAO_IF = '17'
        OR ci.COD_TIPO_CONDICAO_IF = '20'
        OR ci.COD_TIPO_CONDICAO_IF = '21'
        OR ci.COD_TIPO_CONDICAO_IF = '22'
        OR ci.COD_TIPO_CONDICAO_IF = '23'
        OR ci.COD_TIPO_CONDICAO_IF = '24'
      )
    )
),
operation_counts AS (
  SELECT o.NUM_IF, COUNT(*) AS operation_count
  FROM active_operations o
  INNER JOIN candidates c ON c.NUM_IF = o.NUM_IF
  GROUP BY o.NUM_IF
),
dado_counts AS (
  SELECT o.NUM_IF, COUNT(*) AS dado_count
  FROM active_operations o
  INNER JOIN candidates c ON c.NUM_IF = o.NUM_IF
  INNER JOIN dado_rows d ON d.NUM_ID_OPERACAO = o.NUM_ID_OPERACAO
  GROUP BY o.NUM_IF
),
lancamento_counts AS (
  SELECT o.NUM_IF, COUNT(*) AS lancamento_count
  FROM active_operations o
  INNER JOIN candidates c ON c.NUM_IF = o.NUM_IF
  INNER JOIN lancamento_rows l ON l.NUM_ID_OPERACAO = o.NUM_ID_OPERACAO
  GROUP BY o.NUM_IF
),
operation_shape_invalid AS (
  SELECT oc.NUM_IF, 'shape.operacao_dado_lancamento_1_2_1' AS reason
  FROM operation_counts oc
  LEFT JOIN dado_counts dc ON dc.NUM_IF = oc.NUM_IF
  LEFT JOIN lancamento_counts lc ON lc.NUM_IF = oc.NUM_IF
  WHERE COALESCE(dc.dado_count, 0) <> 2 * oc.operation_count
     OR COALESCE(lc.lancamento_count, 0) <> oc.operation_count
),
resgate_counts AS (
  SELECT ci.NUM_IF, COUNT(*) AS resgate_count
  FROM condition_rows ci
  INNER JOIN candidates c ON c.NUM_IF = ci.NUM_IF
  INNER JOIN resgate_rows r
    ON r.NUM_CONDICAO_IF = ci.NUM_CONDICAO_IF
  WHERE ci.DAT_EXCLUSAO IS NULL
    AND r.DAT_EXCLUSAO IS NULL
  GROUP BY ci.NUM_IF
),
resgate_shape_invalid AS (
  SELECT NUM_IF, 'shape.resgate_gt_1' AS reason
  FROM resgate_counts
  WHERE resgate_count > 1
),
duplicate_cod_if_values AS (
  SELECT normalized_cod_if
  FROM root_rows
  WHERE NUM_TIPO_IF = '49'
    AND DAT_EXCLUSAO IS NULL
    AND COD_IF IS NOT NULL
    AND normalized_cod_if <> ''
  GROUP BY normalized_cod_if
  HAVING COUNT(*) > 1
),
cod_if_invalid AS (
  SELECT DISTINCT r.NUM_IF, 'uniqueness.cod_if' AS reason
  FROM root_rows r
  INNER JOIN candidates c ON c.NUM_IF = r.NUM_IF
  INNER JOIN duplicate_cod_if_values d
    ON d.normalized_cod_if = r.normalized_cod_if
),
duplicate_cod_operacao_values AS (
  SELECT normalized_cod_operacao
  FROM operation_rows
  WHERE COD_OPERACAO IS NOT NULL
    AND normalized_cod_operacao <> ''
  GROUP BY normalized_cod_operacao
  HAVING COUNT(*) > 1
),
cod_operacao_invalid AS (
  SELECT DISTINCT o.NUM_IF, 'uniqueness.cod_operacao' AS reason
  FROM operation_rows o
  INNER JOIN duplicate_cod_operacao_values d
    ON d.normalized_cod_operacao = o.normalized_cod_operacao
  WHERE o.NUM_IF IS NOT NULL
    AND o.NUM_IF <> ''
),
meu_numero_rows AS (
  SELECT
    NUM_IF,
    NUM_ID_OPERACAO,
    'P1' AS side,
    DAT_OPERACAO AS operation_date,
    NUM_CONTA_PARTICIPANTE_P1 AS account_value,
    NUM_CONTROLE_LANCAMENTO_P1 AS control_value,
    NUM_ID_TIPO_OPER_OBJETO_SERV AS tos_value
  FROM operation_rows
  UNION ALL
  SELECT
    NUM_IF,
    NUM_ID_OPERACAO,
    'P2' AS side,
    DAT_OPERACAO AS operation_date,
    NUM_CONTA_PARTICIPANTE_P2 AS account_value,
    NUM_CONTROLE_LANCAMENTO_P2 AS control_value,
    NUM_ID_TIPO_OPER_OBJETO_SERV AS tos_value
  FROM operation_rows
),
complete_meu_numero_rows AS (
  SELECT *
  FROM meu_numero_rows
  WHERE operation_date IS NOT NULL
    AND TRIM(CAST(operation_date AS STRING)) <> ''
    AND account_value IS NOT NULL
    AND TRIM(CAST(account_value AS STRING)) <> ''
    AND control_value IS NOT NULL
    AND TRIM(CAST(control_value AS STRING)) <> ''
    AND tos_value IS NOT NULL
    AND TRIM(CAST(tos_value AS STRING)) <> ''
),
duplicate_meu_numero_values AS (
  SELECT operation_date, account_value, control_value, tos_value
  FROM complete_meu_numero_rows
  GROUP BY operation_date, account_value, control_value, tos_value
  HAVING COUNT(*) > 1
),
meu_numero_invalid AS (
  SELECT DISTINCT m.NUM_IF, 'uniqueness.meu_numero' AS reason
  FROM complete_meu_numero_rows m
  INNER JOIN duplicate_meu_numero_values d
    ON d.operation_date = m.operation_date
   AND d.account_value = m.account_value
   AND d.control_value = m.control_value
   AND d.tos_value = m.tos_value
  WHERE m.NUM_IF IS NOT NULL
    AND m.NUM_IF <> ''
),
account_references AS (
  SELECT
    t.NUM_IF,
    CAST(t.NUM_CONTA_PARTICIPANTE AS STRING) AS raw_account,
    CASE
      WHEN TRIM(CAST(t.NUM_CONTA_PARTICIPANTE AS STRING))
        RLIKE '^-?[0-9]+[.][0-9]*0$'
        THEN REGEXP_REPLACE(
          REGEXP_REPLACE(
            TRIM(CAST(t.NUM_CONTA_PARTICIPANTE AS STRING)),
            '([.][0-9]*?)0+$',
            '$1'
          ),
          '[.]$',
          ''
        )
      ELSE TRIM(CAST(t.NUM_CONTA_PARTICIPANTE AS STRING))
    END AS account_id
  FROM title_rows t
  UNION ALL
  SELECT
    d.NUM_IF,
    CAST(d.NUM_CONTA_PARTICIPANTE AS STRING) AS raw_account,
    CASE
      WHEN TRIM(CAST(d.NUM_CONTA_PARTICIPANTE AS STRING))
        RLIKE '^-?[0-9]+[.][0-9]*0$'
        THEN REGEXP_REPLACE(
          REGEXP_REPLACE(
            TRIM(CAST(d.NUM_CONTA_PARTICIPANTE AS STRING)),
            '([.][0-9]*?)0+$',
            '$1'
          ),
          '[.]$',
          ''
        )
      ELSE TRIM(CAST(d.NUM_CONTA_PARTICIPANTE AS STRING))
    END AS account_id
  FROM deposit_rows d
  UNION ALL
  SELECT
    o.NUM_IF,
    CAST(o.NUM_CONTA_PARTICIPANTE_P1 AS STRING) AS raw_account,
    CASE
      WHEN TRIM(CAST(o.NUM_CONTA_PARTICIPANTE_P1 AS STRING))
        RLIKE '^-?[0-9]+[.][0-9]*0$'
        THEN REGEXP_REPLACE(
          REGEXP_REPLACE(
            TRIM(CAST(o.NUM_CONTA_PARTICIPANTE_P1 AS STRING)),
            '([.][0-9]*?)0+$',
            '$1'
          ),
          '[.]$',
          ''
        )
      ELSE TRIM(CAST(o.NUM_CONTA_PARTICIPANTE_P1 AS STRING))
    END AS account_id
  FROM operation_rows o
  UNION ALL
  SELECT
    o.NUM_IF,
    CAST(o.NUM_CONTA_PARTICIPANTE_P2 AS STRING) AS raw_account,
    CASE
      WHEN TRIM(CAST(o.NUM_CONTA_PARTICIPANTE_P2 AS STRING))
        RLIKE '^-?[0-9]+[.][0-9]*0$'
        THEN REGEXP_REPLACE(
          REGEXP_REPLACE(
            TRIM(CAST(o.NUM_CONTA_PARTICIPANTE_P2 AS STRING)),
            '([.][0-9]*?)0+$',
            '$1'
          ),
          '[.]$',
          ''
        )
      ELSE TRIM(CAST(o.NUM_CONTA_PARTICIPANTE_P2 AS STRING))
    END AS account_id
  FROM operation_rows o
),
blank_account_invalid AS (
  SELECT DISTINCT a.NUM_IF, 'lookup.account_blank' AS reason
  FROM account_references a
  INNER JOIN candidates c ON c.NUM_IF = a.NUM_IF
  WHERE a.raw_account IS NOT NULL
    AND TRIM(a.raw_account) = ''
),
nonblank_account_references AS (
  SELECT a.NUM_IF, a.account_id
  FROM account_references a
  INNER JOIN candidates c ON c.NUM_IF = a.NUM_IF
  WHERE a.raw_account IS NOT NULL
    AND TRIM(a.raw_account) <> ''
),
unresolved_account_invalid AS (
  SELECT DISTINCT a.NUM_IF, 'lookup.account_unresolved_or_ineligible' AS reason
  FROM nonblank_account_references a
  LEFT ANTI JOIN eligible_accounts e ON e.account_id = a.account_id
),
operation_lookup_rows AS (
  SELECT
    o.NUM_IF,
    CAST(o.NUM_ID_TIPO_OPER_OBJETO_SERV AS STRING) AS raw_tos_id,
    CASE
      WHEN TRIM(CAST(o.NUM_ID_TIPO_OPER_OBJETO_SERV AS STRING))
        RLIKE '^-?[0-9]+[.][0-9]*0$'
        THEN REGEXP_REPLACE(
          REGEXP_REPLACE(
            TRIM(CAST(o.NUM_ID_TIPO_OPER_OBJETO_SERV AS STRING)),
            '([.][0-9]*?)0+$',
            '$1'
          ),
          '[.]$',
          ''
        )
      ELSE TRIM(CAST(o.NUM_ID_TIPO_OPER_OBJETO_SERV AS STRING))
    END AS tos_id,
    REGEXP_REPLACE(
      TRIM(CAST(o.NUM_ID_MODALIDADE_LIQUIDACAO AS STRING)),
      '[.]0$',
      ''
    ) AS modalidade_id
  FROM operation_rows o
  INNER JOIN candidates c ON c.NUM_IF = o.NUM_IF
),
blank_tos_invalid AS (
  SELECT DISTINCT NUM_IF, 'lookup.operation_tos_blank' AS reason
  FROM operation_lookup_rows
  WHERE raw_tos_id IS NULL OR TRIM(raw_tos_id) = ''
),
nonblank_operation_lookup_rows AS (
  SELECT *
  FROM operation_lookup_rows
  WHERE raw_tos_id IS NOT NULL AND TRIM(raw_tos_id) <> ''
),
invalid_tos AS (
  SELECT DISTINCT o.NUM_IF, 'lookup.operation_tos_invalid' AS reason
  FROM nonblank_operation_lookup_rows o
  LEFT ANTI JOIN valid_tos t ON t.tos_id = o.tos_id
),
sem_modalidade_invalid AS (
  SELECT DISTINCT o.NUM_IF, 'lookup.sem_modalidade' AS reason
  FROM nonblank_operation_lookup_rows o
  LEFT ANTI JOIN valid_sem_modalidade_tos s ON s.tos_id = o.tos_id
  WHERE o.modalidade_id IN ('6', '16')
),
invalid_num_ifs_raw AS (
  SELECT * FROM root_domain_invalid
  UNION ALL SELECT * FROM root_date_invalid
  UNION ALL SELECT * FROM title_invalid
  UNION ALL SELECT * FROM condition_invalid
  UNION ALL SELECT * FROM resgate_domain_invalid
  UNION ALL SELECT * FROM polymorphism_invalid
  UNION ALL SELECT * FROM unknown_condition_type_invalid
  UNION ALL SELECT * FROM operation_shape_invalid
  UNION ALL SELECT * FROM resgate_shape_invalid
  UNION ALL SELECT * FROM cod_if_invalid
  UNION ALL SELECT * FROM cod_operacao_invalid
  UNION ALL SELECT * FROM meu_numero_invalid
  UNION ALL SELECT * FROM blank_account_invalid
  UNION ALL SELECT * FROM unresolved_account_invalid
  UNION ALL SELECT * FROM blank_tos_invalid
  UNION ALL SELECT * FROM invalid_tos
  UNION ALL SELECT * FROM sem_modalidade_invalid
),
invalid_num_ifs AS (
  SELECT DISTINCT NUM_IF, reason
  FROM invalid_num_ifs_raw
  WHERE NUM_IF IS NOT NULL AND NUM_IF <> ''
),
invalid_ids AS (
  SELECT DISTINCT NUM_IF
  FROM invalid_num_ifs
)
SELECT DISTINCT c.NUM_IF
FROM candidates c
LEFT ANTI JOIN invalid_ids i ON i.NUM_IF = c.NUM_IF
;
