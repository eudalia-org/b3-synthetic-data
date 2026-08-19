-- Catálogo único de queries de domínio por produto.
-- O engorda_tables.py seleciona automaticamente o bloco de --produto.
-- Cada query preenchida deve retornar somente uma coluna chamada NUM_IF.
--
-- ===========================================================================
-- ALTERAÇÃO 2026-08: CTE OPER_REGISTRO nos blocos cdb_simplificado,
-- cdb_resgate e cdb_escalonamento.
--
-- MOTIVO: o validador (check_required_lookup_frames / 6.required.operation_tos)
-- exige que TODO instrumento ativo tenha ao menos uma operação de REGISTRO cuja
-- rota resolva para:
--     TIPO_OPERACAO.COD_TIPO_OPERACAO       = '1'
--     TIPO_OPER_OBJETO_SERV.NUM_ID_OBJETO_SERVICO = 44   (objeto de serviço CDB)
--     TIPO_OPER_OBJETO_SERV.IND_DISPONIVEL_IDENTIFICACAO = 'S'
-- O SELECT final destas queries exigia apenas "existe ALGUMA operação com
-- cluster completo (DADO_OPERACAO + LANCAMENTO + ESPECIFICACAO + comitente)",
-- que é condição DIFERENTE e mais fraca. Daí os erros
--     [ERROR] 6.required.operation_tos  (67414 no escalonamento, 60043 no resgate)
--
-- ATENÇÃO — ESTE FILTRO AINDA NÃO FOI VALIDADO CONTRA O DADO:
--   * se OPER_REGISTRO ZERAR (ou reduzir drasticamente) o domínio, então os
--     instrumentos deste produto NÃO usam a rota 44/'S' e o predicado precisa
--     ser revisto junto à B3 — não force o filtro;
--   * o TIPO_OPER_OBJETO_SERV consultado aqui é o da ORIGEM (RAW); o validador
--     resolve o mesmo ID contra o DESTINO (QAB). Se houver divergência de
--     configuração entre os dois ambientes, este filtro passa e o validador
--     continua reprovando — nesse caso o problema é do QAB, não da query.
--
-- PARA REVERTER: remova o CTE OPER_REGISTRO e o respectivo
--     INNER JOIN OPER_REGISTRO ORG ON ORG.NUM_IF = F.NUM_IF
-- do SELECT final. Nada mais depende dele.
--
-- PRÉ-REQUISITO: exige os Parquets RAW de TIPO_OPER_OBJETO_SERV e TIPO_OPERACAO
-- (ambas static no spec). Confirme com um spark.read.parquet nos dois paths
-- antes de rodar valendo.
--
-- NÃO aplicado aos blocos rdb_*: o objeto de serviço do RDB é 45 (não 44) e o
-- validador marca CAP_LOOKUP_TOS como NÃO SUPORTADA para RDB, então esse check
-- sai como WARN e não como ERROR. Aplicar 44 ali seria ativamente errado.
-- ===========================================================================

-- BEGIN QUERY: cdb_simplificado
-- Query Spark SQL que define o domínio de instrumentos a clonar.
-- Contrato: retornar somente uma coluna chamada NUM_IF, sem valores nulos.
-- Use RAW_<TABELA> entre chaves duplas para referenciar uma fonte RAW.

WITH FILTRO_BASE AS (
    SELECT DISTINCT IFE.NUM_IF
    FROM {{RAW_INSTRUMENTO_FINANCEIRO}} IFE
        INNER JOIN {{RAW_TITULO}} TIT
            ON TIT.NUM_IF = IFE.NUM_IF
        INNER JOIN {{RAW_CONDICAO_IF}} CIF
            ON CIF.NUM_IF = IFE.NUM_IF
        INNER JOIN {{RAW_RESGATE}} RES
            ON RES.NUM_CONDICAO_IF = CIF.NUM_CONDICAO_IF
    WHERE IFE.NUM_TIPO_IF = 49
        AND TIT.COD_TIPO_ESCALONAMENTO IS NULL
        AND RES.COD_COND_RESGATE = 'SEM TABELA'
        AND IFE.DAT_EXCLUSAO IS NULL
        AND CIF.DAT_EXCLUSAO IS NULL
        AND RES.DAT_EXCLUSAO IS NULL
),
FLAGS_IF AS (
    SELECT DISTINCT C.NUM_IF
    FROM {{RAW_CONDICAO_IF}} C
        INNER JOIN FILTRO_BASE FB
            ON FB.NUM_IF = C.NUM_IF
    WHERE C.DAT_EXCLUSAO IS NULL
        AND C.COD_TIPO_CONDICAO_IF <> 20
),
DEP_IF AS (
    SELECT DISTINCT DP.NUM_IF
    FROM {{RAW_DEPOSITO_AUTOMATICO_IF}} DP
        INNER JOIN FILTRO_BASE FB
            ON FB.NUM_IF = DP.NUM_IF
    WHERE DP.NUM_IF IS NOT NULL
),
OPER_REGISTRO AS (
    -- Instrumentos que possuem operação de REGISTRO na rota exigida pelo app.
    -- Ver cabeçalho do arquivo antes de alterar/remover.
    SELECT DISTINCT O.NUM_IF
    FROM {{RAW_OPERACAO}} O
        INNER JOIN {{RAW_TIPO_OPER_OBJETO_SERV}} TOS
            ON TOS.NUM_ID_TIPO_OPER_OBJETO_SERV = O.NUM_ID_TIPO_OPER_OBJETO_SERV
        INNER JOIN {{RAW_TIPO_OPERACAO}} TOP
            ON TOP.NUM_ID_TIPO_OPERACAO = TOS.NUM_ID_TIPO_OPERACAO
    WHERE TRIM(TOP.COD_TIPO_OPERACAO) = '1'
        AND TOS.NUM_ID_OBJETO_SERVICO = 44
        AND TRIM(TOS.IND_DISPONIVEL_IDENTIFICACAO) = 'S'
)
SELECT DISTINCT F.NUM_IF
FROM FLAGS_IF F
    INNER JOIN DEP_IF DEP
        ON DEP.NUM_IF = F.NUM_IF
    INNER JOIN OPER_REGISTRO ORG
        ON ORG.NUM_IF = F.NUM_IF
    INNER JOIN {{RAW_OPERACAO}} O
        ON O.NUM_IF = F.NUM_IF
    INNER JOIN {{RAW_DADO_OPERACAO}} DOP
        ON DOP.NUM_ID_OPERACAO = O.NUM_ID_OPERACAO
    INNER JOIN {{RAW_LANCAMENTO}} LAN
        ON LAN.NUM_ID_OPERACAO = O.NUM_ID_OPERACAO
    INNER JOIN {{RAW_ESPECIFICACAO}} ESP
        ON ESP.NUM_ID_OPERACAO = O.NUM_ID_OPERACAO
    INNER JOIN {{RAW_ESPECIFICACAO_COMITENTE}} EPC
        ON EPC.NUM_ID_ESPECIFICACAO = ESP.NUM_ID_ESPECIFICACAO;
-- END QUERY: cdb_simplificado

-- BEGIN QUERY: cdb_resgate
-- Query Spark SQL que define o domínio de CDBs com resgate (CRES) a clonar.
-- Contrato: retornar somente uma coluna chamada NUM_IF, sem valores nulos.
-- Use RAW_<TABELA> entre chaves duplas para referenciar uma fonte RAW.
-- filtros num_tipo_if 49 e cod_cond_resgate  mercado,com tabela e especifica e tipo escalonamento nulo

WITH FILTRO_BASE AS (
    SELECT DISTINCT IFE.NUM_IF
    FROM {{RAW_INSTRUMENTO_FINANCEIRO}} IFE
        INNER JOIN {{RAW_TITULO}} TIT
            ON TIT.NUM_IF = IFE.NUM_IF
        INNER JOIN {{RAW_CONDICAO_IF}} CIF
            ON CIF.NUM_IF = IFE.NUM_IF
        INNER JOIN {{RAW_RESGATE}} RES
            ON RES.NUM_CONDICAO_IF = CIF.NUM_CONDICAO_IF
    WHERE IFE.NUM_TIPO_IF = 49
        AND RES.COD_COND_RESGATE IN (
            'MERCADO',
            'COM TABELA',
            'ESPECIFICA'
        )
        AND TIT.COD_TIPO_ESCALONAMENTO IS NULL
        AND IFE.DAT_EXCLUSAO IS NULL
        AND CIF.DAT_EXCLUSAO IS NULL
        AND RES.DAT_EXCLUSAO IS NULL
),
FLAGS_IF AS (
    SELECT DISTINCT C.NUM_IF
    FROM {{RAW_CONDICAO_IF}} C
        INNER JOIN FILTRO_BASE FB
            ON FB.NUM_IF = C.NUM_IF
    WHERE C.DAT_EXCLUSAO IS NULL
        AND C.COD_TIPO_CONDICAO_IF <> 20
),
DEP_IF AS (
    SELECT DISTINCT DP.NUM_IF
    FROM {{RAW_DEPOSITO_AUTOMATICO_IF}} DP
        INNER JOIN FILTRO_BASE FB
            ON FB.NUM_IF = DP.NUM_IF
    WHERE DP.NUM_IF IS NOT NULL
),
OPER_REGISTRO AS (
    -- Instrumentos que possuem operação de REGISTRO na rota exigida pelo app.
    -- Ver cabeçalho do arquivo antes de alterar/remover.
    SELECT DISTINCT O.NUM_IF
    FROM {{RAW_OPERACAO}} O
        INNER JOIN {{RAW_TIPO_OPER_OBJETO_SERV}} TOS
            ON TOS.NUM_ID_TIPO_OPER_OBJETO_SERV = O.NUM_ID_TIPO_OPER_OBJETO_SERV
        INNER JOIN {{RAW_TIPO_OPERACAO}} TOP
            ON TOP.NUM_ID_TIPO_OPERACAO = TOS.NUM_ID_TIPO_OPERACAO
    WHERE TRIM(TOP.COD_TIPO_OPERACAO) = '1'
        AND TOS.NUM_ID_OBJETO_SERVICO = 44
        AND TRIM(TOS.IND_DISPONIVEL_IDENTIFICACAO) = 'S'
)
SELECT DISTINCT F.NUM_IF
FROM FLAGS_IF F
    INNER JOIN DEP_IF DEP
        ON DEP.NUM_IF = F.NUM_IF
    INNER JOIN OPER_REGISTRO ORG
        ON ORG.NUM_IF = F.NUM_IF
    INNER JOIN {{RAW_OPERACAO}} O
        ON O.NUM_IF = F.NUM_IF
    INNER JOIN {{RAW_DADO_OPERACAO}} DOP
        ON DOP.NUM_ID_OPERACAO = O.NUM_ID_OPERACAO
    INNER JOIN {{RAW_LANCAMENTO}} LAN
        ON LAN.NUM_ID_OPERACAO = O.NUM_ID_OPERACAO
    INNER JOIN {{RAW_ESPECIFICACAO}} ESP
        ON ESP.NUM_ID_OPERACAO = O.NUM_ID_OPERACAO
    INNER JOIN {{RAW_ESPECIFICACAO_COMITENTE}} EPC
        ON EPC.NUM_ID_ESPECIFICACAO = ESP.NUM_ID_ESPECIFICACAO;
-- END QUERY: cdb_resgate

-- BEGIN QUERY: cdb_escalonamento
-- Query Spark SQL que define o domínio de CDBs com escalonamento (CESC) a clonar.
-- Contrato: retornar somente uma coluna chamada NUM_IF, sem valores nulos.
-- Use RAW_<TABELA> entre chaves duplas para referenciar uma fonte RAW.
-- filtros num_tipo_if 49 e cod_tipo_escalonamento não nulo

WITH FILTRO_BASE AS (
    SELECT DISTINCT IFE.NUM_IF
    FROM {{RAW_INSTRUMENTO_FINANCEIRO}} IFE
        INNER JOIN {{RAW_TITULO}} TIT
            ON TIT.NUM_IF = IFE.NUM_IF
        INNER JOIN {{RAW_CONDICAO_IF}} CIF
            ON CIF.NUM_IF = IFE.NUM_IF
        INNER JOIN {{RAW_RESGATE}} RES
            ON RES.NUM_CONDICAO_IF = CIF.NUM_CONDICAO_IF
    WHERE IFE.NUM_TIPO_IF = 49
        AND RES.COD_COND_RESGATE IN ('SEM TABELA')
        AND TIT.COD_TIPO_ESCALONAMENTO IS NOT NULL
        AND IFE.DAT_EXCLUSAO IS NULL
        AND CIF.DAT_EXCLUSAO IS NULL
        AND RES.DAT_EXCLUSAO IS NULL
),
FLAGS_IF AS (
    SELECT DISTINCT C.NUM_IF
    FROM {{RAW_CONDICAO_IF}} C
        INNER JOIN FILTRO_BASE FB
            ON FB.NUM_IF = C.NUM_IF
    WHERE C.DAT_EXCLUSAO IS NULL
        AND C.COD_TIPO_CONDICAO_IF <> 20
),
DEP_IF AS (
    SELECT DISTINCT DP.NUM_IF
    FROM {{RAW_DEPOSITO_AUTOMATICO_IF}} DP
        INNER JOIN FILTRO_BASE FB
            ON FB.NUM_IF = DP.NUM_IF
    WHERE DP.NUM_IF IS NOT NULL
),
OPER_REGISTRO AS (
    -- Instrumentos que possuem operação de REGISTRO na rota exigida pelo app.
    -- Ver cabeçalho do arquivo antes de alterar/remover.
    SELECT DISTINCT O.NUM_IF
    FROM {{RAW_OPERACAO}} O
        INNER JOIN {{RAW_TIPO_OPER_OBJETO_SERV}} TOS
            ON TOS.NUM_ID_TIPO_OPER_OBJETO_SERV = O.NUM_ID_TIPO_OPER_OBJETO_SERV
        INNER JOIN {{RAW_TIPO_OPERACAO}} TOP
            ON TOP.NUM_ID_TIPO_OPERACAO = TOS.NUM_ID_TIPO_OPERACAO
    WHERE TRIM(TOP.COD_TIPO_OPERACAO) = '1'
        AND TOS.NUM_ID_OBJETO_SERVICO = 44
        AND TRIM(TOS.IND_DISPONIVEL_IDENTIFICACAO) = 'S'
)
SELECT DISTINCT F.NUM_IF
FROM FLAGS_IF F
    INNER JOIN DEP_IF DEP
        ON DEP.NUM_IF = F.NUM_IF
    INNER JOIN OPER_REGISTRO ORG
        ON ORG.NUM_IF = F.NUM_IF
    INNER JOIN {{RAW_OPERACAO}} O
        ON O.NUM_IF = F.NUM_IF
    INNER JOIN {{RAW_DADO_OPERACAO}} DOP
        ON DOP.NUM_ID_OPERACAO = O.NUM_ID_OPERACAO
    INNER JOIN {{RAW_LANCAMENTO}} LAN
        ON LAN.NUM_ID_OPERACAO = O.NUM_ID_OPERACAO
    INNER JOIN {{RAW_ESPECIFICACAO}} ESP
        ON ESP.NUM_ID_OPERACAO = O.NUM_ID_OPERACAO
    INNER JOIN {{RAW_ESPECIFICACAO_COMITENTE}} EPC
        ON EPC.NUM_ID_ESPECIFICACAO = ESP.NUM_ID_ESPECIFICACAO;
-- END QUERY: cdb_escalonamento

-- BEGIN QUERY: rdb_inclusao
-- Query Spark SQL que define o domínio de RDBs simplificados (INCL) a clonar.
-- Contrato: retornar somente uma coluna chamada NUM_IF, sem valores nulos.
-- Use RAW_<TABELA> entre chaves duplas para referenciar uma fonte RAW.
-- filtros num_tipo_if 50 e cod_cond_resgate sem tabela e tipo escalonamento nulo
-- NB: sem OPER_REGISTRO — objeto de serviço do RDB é 45, e o validador não
-- suporta a checagem de rota para RDB (CAP_LOOKUP_TOS não suportada -> WARN).
WITH FILTRO_BASE AS (
    SELECT DISTINCT IFE.NUM_IF
    FROM {{RAW_INSTRUMENTO_FINANCEIRO}} IFE
        INNER JOIN {{RAW_TITULO}} TIT
            ON TIT.NUM_IF = IFE.NUM_IF
        INNER JOIN {{RAW_CONDICAO_IF}} CIF
            ON CIF.NUM_IF = IFE.NUM_IF
        INNER JOIN {{RAW_RESGATE}} RES
            ON RES.NUM_CONDICAO_IF = CIF.NUM_CONDICAO_IF
    WHERE IFE.NUM_TIPO_IF = 50
        AND RES.COD_COND_RESGATE IN ('SEM TABELA')
        AND TIT.COD_TIPO_ESCALONAMENTO IS NULL
        AND IFE.DAT_EXCLUSAO IS NULL
        AND CIF.DAT_EXCLUSAO IS NULL
        AND RES.DAT_EXCLUSAO IS NULL
),
FLAGS_IF AS (
    SELECT DISTINCT C.NUM_IF
    FROM {{RAW_CONDICAO_IF}} C
        INNER JOIN FILTRO_BASE FB
            ON FB.NUM_IF = C.NUM_IF
    WHERE C.DAT_EXCLUSAO IS NULL
        AND C.COD_TIPO_CONDICAO_IF <> 20
),
DEP_IF AS (
    SELECT DISTINCT DP.NUM_IF
    FROM {{RAW_DEPOSITO_AUTOMATICO_IF}} DP
        INNER JOIN FILTRO_BASE FB
            ON FB.NUM_IF = DP.NUM_IF
    WHERE DP.NUM_IF IS NOT NULL
)
SELECT DISTINCT F.NUM_IF
FROM FLAGS_IF F
    INNER JOIN DEP_IF DEP
        ON DEP.NUM_IF = F.NUM_IF
    INNER JOIN {{RAW_OPERACAO}} O
        ON O.NUM_IF = F.NUM_IF
    INNER JOIN {{RAW_DADO_OPERACAO}} DOP
        ON DOP.NUM_ID_OPERACAO = O.NUM_ID_OPERACAO
    INNER JOIN {{RAW_LANCAMENTO}} LAN
        ON LAN.NUM_ID_OPERACAO = O.NUM_ID_OPERACAO
    INNER JOIN {{RAW_ESPECIFICACAO}} ESP
        ON ESP.NUM_ID_OPERACAO = O.NUM_ID_OPERACAO
    INNER JOIN {{RAW_ESPECIFICACAO_COMITENTE}} EPC
        ON EPC.NUM_ID_ESPECIFICACAO = ESP.NUM_ID_ESPECIFICACAO;
-- END QUERY: rdb_inclusao

-- BEGIN QUERY: rdb_resgate
-- Query Spark SQL que define o domínio de RDBs com resgate (CRESG) a clonar.
-- Contrato: retornar somente uma coluna chamada NUM_IF, sem valores nulos.
-- Use RAW_<TABELA> entre chaves duplas para referenciar uma fonte RAW.
-- filtros num_tipo_if 50 e cod_cond_resgate  mercado,com tabela e especifica e tipo escalonamento nulo
-- NB: sem OPER_REGISTRO — ver bloco rdb_inclusao.
WITH FILTRO_BASE AS (
    SELECT DISTINCT IFE.NUM_IF
    FROM {{RAW_INSTRUMENTO_FINANCEIRO}} IFE
        INNER JOIN {{RAW_TITULO}} TIT
            ON TIT.NUM_IF = IFE.NUM_IF
        INNER JOIN {{RAW_CONDICAO_IF}} CIF
            ON CIF.NUM_IF = IFE.NUM_IF
        INNER JOIN {{RAW_RESGATE}} RES
            ON RES.NUM_CONDICAO_IF = CIF.NUM_CONDICAO_IF
    WHERE IFE.NUM_TIPO_IF = 50
        AND RES.COD_COND_RESGATE IN (
            'MERCADO',
            'COM TABELA',
            'ESPECIFICA'
        )
        AND TIT.COD_TIPO_ESCALONAMENTO IS NULL
        AND IFE.DAT_EXCLUSAO IS NULL
        AND CIF.DAT_EXCLUSAO IS NULL
        AND RES.DAT_EXCLUSAO IS NULL
),
FLAGS_IF AS (
    SELECT DISTINCT C.NUM_IF
    FROM {{RAW_CONDICAO_IF}} C
        INNER JOIN FILTRO_BASE FB
            ON FB.NUM_IF = C.NUM_IF
    WHERE C.DAT_EXCLUSAO IS NULL
        AND C.COD_TIPO_CONDICAO_IF <> 20
),
DEP_IF AS (
    SELECT DISTINCT DP.NUM_IF
    FROM {{RAW_DEPOSITO_AUTOMATICO_IF}} DP
        INNER JOIN FILTRO_BASE FB
            ON FB.NUM_IF = DP.NUM_IF
    WHERE DP.NUM_IF IS NOT NULL
)
SELECT DISTINCT F.NUM_IF
FROM FLAGS_IF F
    INNER JOIN DEP_IF DEP
        ON DEP.NUM_IF = F.NUM_IF
    INNER JOIN {{RAW_OPERACAO}} O
        ON O.NUM_IF = F.NUM_IF
    INNER JOIN {{RAW_DADO_OPERACAO}} DOP
        ON DOP.NUM_ID_OPERACAO = O.NUM_ID_OPERACAO
    INNER JOIN {{RAW_LANCAMENTO}} LAN
        ON LAN.NUM_ID_OPERACAO = O.NUM_ID_OPERACAO
    INNER JOIN {{RAW_ESPECIFICACAO}} ESP
        ON ESP.NUM_ID_OPERACAO = O.NUM_ID_OPERACAO
    INNER JOIN {{RAW_ESPECIFICACAO_COMITENTE}} EPC
        ON EPC.NUM_ID_ESPECIFICACAO = ESP.NUM_ID_ESPECIFICACAO;
-- END QUERY: rdb_resgate

-- BEGIN QUERY: lci
-- Query Spark SQL que define o domínio de LCI simplificados (INCL) a clonar.
-- Contrato: retornar somente uma coluna chamada NUM_IF, sem valores nulos.
-- Use RAW_<TABELA> entre chaves duplas para referenciar uma fonte RAW.
-- filtros num_tipo_if 81 e cod_cond_resgate sem tabela e tipo escalonamento nulo
WITH FILTRO_BASE AS (
    SELECT DISTINCT IFE.NUM_IF
    FROM {{RAW_INSTRUMENTO_FINANCEIRO}} IFE
        INNER JOIN {{RAW_TITULO}} TIT
            ON TIT.NUM_IF = IFE.NUM_IF
        INNER JOIN {{RAW_CONDICAO_IF}} CIF
            ON CIF.NUM_IF = IFE.NUM_IF
        INNER JOIN {{RAW_RESGATE}} RES
            ON RES.NUM_CONDICAO_IF = CIF.NUM_CONDICAO_IF
    WHERE IFE.NUM_TIPO_IF = 81
        AND RES.COD_COND_RESGATE IN ('SEM TABELA')
        AND TIT.COD_TIPO_ESCALONAMENTO IS NULL
        AND IFE.DAT_EXCLUSAO IS NULL
        AND CIF.DAT_EXCLUSAO IS NULL
        AND RES.DAT_EXCLUSAO IS NULL
),
FLAGS_IF AS (
    SELECT DISTINCT C.NUM_IF
    FROM {{RAW_CONDICAO_IF}} C
        INNER JOIN FILTRO_BASE FB
            ON FB.NUM_IF = C.NUM_IF
    WHERE C.DAT_EXCLUSAO IS NULL
        AND C.COD_TIPO_CONDICAO_IF <> 20
),
DEP_IF AS (
    SELECT DISTINCT DP.NUM_IF
    FROM {{RAW_DEPOSITO_AUTOMATICO_IF}} DP
        INNER JOIN FILTRO_BASE FB
            ON FB.NUM_IF = DP.NUM_IF
    WHERE DP.NUM_IF IS NOT NULL
)
SELECT DISTINCT F.NUM_IF
FROM FLAGS_IF F
    INNER JOIN DEP_IF DEP
        ON DEP.NUM_IF = F.NUM_IF
    INNER JOIN {{RAW_OPERACAO}} O
        ON O.NUM_IF = F.NUM_IF
    INNER JOIN {{RAW_DADO_OPERACAO}} DOP
        ON DOP.NUM_ID_OPERACAO = O.NUM_ID_OPERACAO
    INNER JOIN {{RAW_LANCAMENTO}} LAN
        ON LAN.NUM_ID_OPERACAO = O.NUM_ID_OPERACAO
    INNER JOIN {{RAW_ESPECIFICACAO}} ESP
        ON ESP.NUM_ID_OPERACAO = O.NUM_ID_OPERACAO
    INNER JOIN {{RAW_ESPECIFICACAO_COMITENTE}} EPC
        ON EPC.NUM_ID_ESPECIFICACAO = ESP.NUM_ID_ESPECIFICACAO;
-- END QUERY: lci

-- BEGIN QUERY: lca
-- Query Spark SQL que define o domínio de LCA simplificados (INCL) a clonar.
-- Contrato: retornar somente uma coluna chamada NUM_IF, sem valores nulos.
-- Use RAW_<TABELA> entre chaves duplas para referenciar uma fonte RAW.
-- filtros num_tipo_if 96 e cod_cond_resgate sem tabela e tipo escalonamento nulo
WITH FILTRO_BASE AS (
    SELECT DISTINCT IFE.NUM_IF
    FROM {{RAW_INSTRUMENTO_FINANCEIRO}} IFE
        INNER JOIN {{RAW_TITULO}} TIT
            ON TIT.NUM_IF = IFE.NUM_IF
        INNER JOIN {{RAW_CONDICAO_IF}} CIF
            ON CIF.NUM_IF = IFE.NUM_IF
        INNER JOIN {{RAW_RESGATE}} RES
            ON RES.NUM_CONDICAO_IF = CIF.NUM_CONDICAO_IF
    WHERE IFE.NUM_TIPO_IF = 96
        AND RES.COD_COND_RESGATE IN ('SEM TABELA')
        AND TIT.COD_TIPO_ESCALONAMENTO IS NULL
        AND IFE.DAT_EXCLUSAO IS NULL
        AND CIF.DAT_EXCLUSAO IS NULL
        AND RES.DAT_EXCLUSAO IS NULL
),
FLAGS_IF AS (
    SELECT DISTINCT C.NUM_IF
    FROM {{RAW_CONDICAO_IF}} C
        INNER JOIN FILTRO_BASE FB
            ON FB.NUM_IF = C.NUM_IF
    WHERE C.DAT_EXCLUSAO IS NULL
        AND C.COD_TIPO_CONDICAO_IF <> 20
),
DEP_IF AS (
    SELECT DISTINCT DP.NUM_IF
    FROM {{RAW_DEPOSITO_AUTOMATICO_IF}} DP
        INNER JOIN FILTRO_BASE FB
            ON FB.NUM_IF = DP.NUM_IF
    WHERE DP.NUM_IF IS NOT NULL
)
SELECT DISTINCT F.NUM_IF
FROM FLAGS_IF F
    INNER JOIN DEP_IF DEP
        ON DEP.NUM_IF = F.NUM_IF
    INNER JOIN {{RAW_OPERACAO}} O
        ON O.NUM_IF = F.NUM_IF
    INNER JOIN {{RAW_DADO_OPERACAO}} DOP
        ON DOP.NUM_ID_OPERACAO = O.NUM_ID_OPERACAO
    INNER JOIN {{RAW_LANCAMENTO}} LAN
        ON LAN.NUM_ID_OPERACAO = O.NUM_ID_OPERACAO
    INNER JOIN {{RAW_ESPECIFICACAO}} ESP
        ON ESP.NUM_ID_OPERACAO = O.NUM_ID_OPERACAO
    INNER JOIN {{RAW_ESPECIFICACAO_COMITENTE}} EPC
        ON EPC.NUM_ID_ESPECIFICACAO = ESP.NUM_ID_ESPECIFICACAO;
-- END QUERY: lca

-- BEGIN QUERY: ccb_pppre
-- CCB PPPRE: pagamento de parcelas com indexador prefixado.
SELECT DISTINCT I.NUM_IF
FROM {{RAW_INSTRUMENTO_FINANCEIRO}} I
    INNER JOIN {{RAW_FORMA_PAGAMENTO}} FP
        ON FP.NUM_ID_FORMA_PAGAMENTO = I.NUM_ID_FORMA_PAGAMENTO
    INNER JOIN {{RAW_ACTPCCB_CONDICAO_IF}} ACIF
        ON ACIF.NUM_IF = I.NUM_IF
WHERE I.NUM_TIPO_IF = 53
    AND I.NUM_IF_PERTENCE IS NULL
    AND I.DAT_EXCLUSAO IS NULL
    AND UPPER(TRIM(ACIF.RENT_INDEXADOR_TAXA_FLU)) = 'PREFIXADO'
    AND UPPER(TRIM(ACIF.FORMA_PAGAMENTO)) = 'PAGAMENTO DE PARCELAS';
-- END QUERY: ccb_pppre

-- BEGIN QUERY: ccb_pfpre
-- CCB PFPRE: pagamento de parcelas fixas com indexador prefixado.
SELECT DISTINCT I.NUM_IF
FROM {{RAW_INSTRUMENTO_FINANCEIRO}} I
    INNER JOIN {{RAW_FORMA_PAGAMENTO}} FP
        ON FP.NUM_ID_FORMA_PAGAMENTO = I.NUM_ID_FORMA_PAGAMENTO
    INNER JOIN {{RAW_ACTPCCB_CONDICAO_IF}} ACIF
        ON ACIF.NUM_IF = I.NUM_IF
WHERE I.NUM_TIPO_IF = 53
    AND I.NUM_IF_PERTENCE IS NULL
    AND I.DAT_EXCLUSAO IS NULL
    AND UPPER(TRIM(ACIF.RENT_INDEXADOR_TAXA_FLU)) = 'PREFIXADO'
    AND UPPER(TRIM(ACIF.FORMA_PAGAMENTO)) = 'PAGAMENTO DE PARCELAS FIXAS';
-- END QUERY: ccb_pfpre

-- BEGIN QUERY: ccb_pgrpre
-- CCB PGRPRE: pagamento de rendimento prefixado com indexador VCP.
SELECT DISTINCT I.NUM_IF
FROM {{RAW_INSTRUMENTO_FINANCEIRO}} I
    INNER JOIN {{RAW_FORMA_PAGAMENTO}} FP
        ON FP.NUM_ID_FORMA_PAGAMENTO = I.NUM_ID_FORMA_PAGAMENTO
    INNER JOIN {{RAW_ACTPCCB_CONDICAO_IF}} ACIF
        ON ACIF.NUM_IF = I.NUM_IF
WHERE I.NUM_TIPO_IF = 53
    AND I.NUM_IF_PERTENCE IS NULL
    AND I.DAT_EXCLUSAO IS NULL
    AND UPPER(TRIM(ACIF.RENT_INDEXADOR_TAXA_FLU)) = 'VCP'
    AND UPPER(TRIM(ACIF.FORMA_PAGAMENTO)) = 'PAGAMENTO DE RENDIMENTO PREFIXADO';
-- END QUERY: ccb_pgrpre

-- BEGIN QUERY: ccb_favcp
-- CCB FAVCP: liquidação fora do âmbito B3 com indexador VCP.
SELECT DISTINCT I.NUM_IF
FROM {{RAW_INSTRUMENTO_FINANCEIRO}} I
    INNER JOIN {{RAW_FORMA_PAGAMENTO}} FP
        ON FP.NUM_ID_FORMA_PAGAMENTO = I.NUM_ID_FORMA_PAGAMENTO
    INNER JOIN {{RAW_ACTPCCB_CONDICAO_IF}} ACIF
        ON ACIF.NUM_IF = I.NUM_IF
WHERE I.NUM_TIPO_IF = 53
    AND I.NUM_IF_PERTENCE IS NULL
    AND I.DAT_EXCLUSAO IS NULL
    AND UPPER(TRIM(ACIF.RENT_INDEXADOR_TAXA_FLU)) = 'VCP'
    AND UPPER(TRIM(ACIF.FORMA_PAGAMENTO)) = 'LIQUIDAÇÃO FORA DO ÂMBITO B3';
-- END QUERY: ccb_favcp

-- BEGIN QUERY: ccb_fapre
-- CCB FAPRE: liquidação fora do âmbito B3 com indexador prefixado.
SELECT DISTINCT I.NUM_IF
FROM {{RAW_INSTRUMENTO_FINANCEIRO}} I
    INNER JOIN {{RAW_FORMA_PAGAMENTO}} FP
        ON FP.NUM_ID_FORMA_PAGAMENTO = I.NUM_ID_FORMA_PAGAMENTO
    INNER JOIN {{RAW_ACTPCCB_CONDICAO_IF}} ACIF
        ON ACIF.NUM_IF = I.NUM_IF
WHERE I.NUM_TIPO_IF = 53
    AND I.NUM_IF_PERTENCE IS NULL
    AND I.DAT_EXCLUSAO IS NULL
    AND UPPER(TRIM(ACIF.RENT_INDEXADOR_TAXA_FLU)) = 'PREFIXADO'
    AND UPPER(TRIM(ACIF.FORMA_PAGAMENTO)) = 'LIQUIDAÇÃO FORA DO ÂMBITO B3';
-- END QUERY: ccb_fapre

-- BEGIN QUERY: gravame

-- END QUERY: gravame

-- BEGIN QUERY: lastro

-- END QUERY: lastro

-- BEGIN QUERY: direito_creditorio

-- END QUERY: direito_creditorio
