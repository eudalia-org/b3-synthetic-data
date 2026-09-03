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
        INNER JOIN {{RAW_OPERACAO}} OPER
            ON OPER.NUM_IF = IFE.NUM_IF
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
        AND TRY_CAST(TIT.QTD_RESGATADA AS DECIMAL(38, 18)) = 0
        AND OPER.COD_SITUACAO_OPERACAO = 43
        AND OPER.COD_CONTA_PARTE LIKE "%10-%"
        AND OPER.COD_CONTA_CONTRAPARTE LIKE "%40-%"
        AND OPER.NUM_ID_TIPO_OPER_OBJETO_SERV = 4509
),
OPERACAO_FORA_ROTA AS (
    SELECT DISTINCT O.NUM_IF
    FROM {{RAW_OPERACAO}} O
    WHERE O.NUM_ID_TIPO_OPER_OBJETO_SERV IS NULL
        OR O.NUM_ID_TIPO_OPER_OBJETO_SERV <> 4509
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
    LEFT ANTI JOIN OPERACAO_FORA_ROTA OFR
        ON OFR.NUM_IF = F.NUM_IF
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
        INNER JOIN {{RAW_OPERACAO}} OPER
            ON OPER.NUM_IF = IFE.NUM_IF
    WHERE IFE.NUM_TIPO_IF = 49
        AND RES.COD_COND_RESGATE IN ('SEM TABELA')
        AND TIT.COD_TIPO_ESCALONAMENTO IS NOT NULL
        AND TRY_CAST(TIT.QTD_RESGATADA AS DECIMAL(38, 18)) = 0
        AND IFE.DAT_EXCLUSAO IS NULL
        AND CIF.DAT_EXCLUSAO IS NULL
        AND RES.DAT_EXCLUSAO IS NULL
        AND OPER.COD_SITUACAO_OPERACAO = 43
        AND OPER.COD_CONTA_PARTE LIKE "%10-%"
        AND OPER.COD_CONTA_CONTRAPARTE LIKE "%40-%"
        AND OPER.NUM_ID_TIPO_OPER_OBJETO_SERV = 4509
),
OPERACAO_FORA_ROTA AS (
    SELECT DISTINCT O.NUM_IF
    FROM {{RAW_OPERACAO}} O
    WHERE O.NUM_ID_TIPO_OPER_OBJETO_SERV IS NULL
        OR O.NUM_ID_TIPO_OPER_OBJETO_SERV <> 4509
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
    LEFT ANTI JOIN OPERACAO_FORA_ROTA OFR
        ON OFR.NUM_IF = F.NUM_IF
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
        INNER JOIN {{RAW_OPERACAO}} OPER
            ON OPER.NUM_IF = IFE.NUM_IF
    WHERE IFE.NUM_TIPO_IF = 50
        AND RES.COD_COND_RESGATE IN ('SEM TABELA')
        AND TIT.COD_TIPO_ESCALONAMENTO IS NULL
        AND IFE.DAT_EXCLUSAO IS NULL
        AND CIF.DAT_EXCLUSAO IS NULL
        AND RES.DAT_EXCLUSAO IS NULL
        AND TIT.QTD_RESGATADA = 0
        AND OPER.COD_SITUACAO_OPERACAO = 43
        AND OPER.COD_CONTA_PARTE LIKE "%10-%"
        AND OPER.COD_CONTA_CONTRAPARTE LIKE "%40-%"
        AND OPER.NUM_ID_TIPO_OPER_OBJETO_SERV = 5177
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
        INNER JOIN {{RAW_OPERACAO}} OPER
            ON OPER.NUM_IF = IFE.NUM_IF
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
        AND TRY_CAST(TIT.QTD_RESGATADA AS DECIMAL(38, 18)) = 0
        AND OPER.COD_SITUACAO_OPERACAO = 43
        AND OPER.COD_CONTA_PARTE LIKE "%10-%"
        AND OPER.COD_CONTA_CONTRAPARTE LIKE "%40-%"
        AND OPER.NUM_ID_TIPO_OPER_OBJETO_SERV = 5177
),
OPERACAO_FORA_ROTA AS (
    SELECT DISTINCT O.NUM_IF
    FROM {{RAW_OPERACAO}} O
    WHERE O.NUM_ID_TIPO_OPER_OBJETO_SERV IS NULL
        OR O.NUM_ID_TIPO_OPER_OBJETO_SERV <> 5177
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
    LEFT ANTI JOIN OPERACAO_FORA_ROTA OFR
        ON OFR.NUM_IF = F.NUM_IF
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
        INNER JOIN {{RAW_OPERACAO}} OPER
            ON OPER.NUM_IF = IFE.NUM_IF
    WHERE IFE.NUM_TIPO_IF = 81
        AND RES.COD_COND_RESGATE IN ('SEM TABELA')
        AND TIT.COD_TIPO_ESCALONAMENTO IS NULL
        AND IFE.DAT_EXCLUSAO IS NULL
        AND CIF.DAT_EXCLUSAO IS NULL
        AND RES.DAT_EXCLUSAO IS NULL
        AND OPER.COD_SITUACAO_OPERACAO = 43
        AND OPER.COD_CONTA_PARTE LIKE "%10-%"
        AND OPER.COD_CONTA_CONTRAPARTE LIKE "%40-%"
        AND OPER.NUM_ID_TIPO_OPER_OBJETO_SERV = 3949
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
ROTA_ELEGIVEL AS (
    -- Ver cabeçalho acima antes de alterar/remover.
    SELECT DISTINCT TOS.NUM_ID_TIPO_OPER_OBJETO_SERV
    FROM {{RAW_TIPO_OPER_OBJETO_SERV}} TOS
        INNER JOIN {{RAW_TIPO_OPERACAO}} TOP
            ON TOP.NUM_ID_TIPO_OPERACAO = TOS.NUM_ID_TIPO_OPERACAO
    WHERE TOS.NUM_ID_OBJETO_SERVICO = 75
        AND TRIM(TOP.COD_TIPO_OPERACAO) = '1'
        AND TRIM(TOS.IND_DISPONIVEL_IDENTIFICACAO) = 'S'
),
ROTA_INVALIDA AS (
    -- Basta UMA operação fora da rota elegível para reprovar o instrumento
    -- inteiro, porque o check é linha a linha sobre OPERACAO.
    SELECT DISTINCT O.NUM_IF
    FROM {{RAW_OPERACAO}} O
        LEFT JOIN ROTA_ELEGIVEL RE
            ON RE.NUM_ID_TIPO_OPER_OBJETO_SERV = O.NUM_ID_TIPO_OPER_OBJETO_SERV
    WHERE RE.NUM_ID_TIPO_OPER_OBJETO_SERV IS NULL
),
LOTE_ELEGIVEL AS (
    -- 6e.lookup.lot: INSTRUMENTO_FINANCEIRO.NUM_ID_LOTE precisa resolver para um
    -- LOTE de NUM_ID_TIPO_LOTE = 1 com conta participante.
    -- NB: o validador também exige o lote ATIVO no destino; não filtro
    -- DAT_EXCLUSAO aqui porque a coluna não está declarada no spec de LOTE.
    SELECT DISTINCT L.NUM_ID_LOTE
    FROM {{RAW_LOTE}} L
    WHERE L.NUM_ID_TIPO_LOTE = 1
        AND L.NUM_CONTA_PARTICIPANTE IS NOT NULL
),
LOTE_COM_LASTRO AS (
    -- VINCULO DE LASTRO. O LCI sintetico preserva o NUM_ID_LOTE
    -- original (LOTE e static no fecho deste produto), entao so pode
    -- entrar instrumento cujo lote tenha CREDITO_SCR ativo para semear.
    -- Sem isso a semente lateral nao tem de onde tirar linha e o
    -- sintetico nasceria sem lastro.
    -- MEDIDO no QAB: custa 106 de 5.421.677 LCIs (0,002%).
    SELECT DISTINCT S.NUM_ID_LOTE
    FROM {{RAW_CREDITO_SCR}} S
        INNER JOIN {{RAW_HISTORICO_CREDITO_SCR}} H
            ON H.NUM_ID_CREDITO_SCR = S.NUM_ID_CREDITO_SCR
    WHERE S.DAT_EXCLUSAO IS NULL
        AND S.NUM_ID_LOTE IS NOT NULL
),
CARTEIRA_DUPLICADA AS (
    -- 6e.wallet.*.local: a chave natural da carteira precisa ser única por
    -- instrumento. Quando a origem já tem duas linhas com a mesma chave, o
    -- clone herda a duplicidade — então o instrumento sai do domínio.
    SELECT DISTINCT NUM_IF FROM (
        SELECT CC.NUM_IF
        FROM {{RAW_CARTEIRA_COMITENTE}} CC
        GROUP BY CC.NUM_IF, CC.NUM_ID_ENTIDADE, CC.COD_TIPO_POSICAO_CARTEIRA,
                 CC.NUM_SISTEMA, CC.NUM_CONTA_PARTICIPANTE
        HAVING COUNT(*) > 1
        UNION ALL
        SELECT CP.NUM_IF
        FROM {{RAW_CARTEIRA_PARTICIPANTE}} CP
        GROUP BY CP.NUM_IF, CP.COD_TIPO_POSICAO_CARTEIRA, CP.NUM_SISTEMA,
                 CP.NUM_CONTA_PARTICIPANTE
        HAVING COUNT(*) > 1
    ) D
)
-- ===========================================================================
-- ROTA DE OPERAÇÃO (6e/6g.lookup.route).
--
-- O check exige que TODA operação sintética use rota elegível — ele roda sobre
-- `tables["OPERACAO"]` inteira, sem recorte por tipo de operação:
--     bad = operations.join(eligible_routes, "route_id", "left_anti")
-- Rota elegível = objeto de serviço 75 (LCI) / 843 (LCA) + COD_TIPO_OPERACAO='1'
-- + IND_DISPONIVEL_IDENTIFICACAO='S'.
--
-- MEDIDO no QAB (LCI): existe UMA única rota elegível, e mesmo assim
--     LCIs ativas ..................................... 5.761.483
--     LCIs com TODAS as operações em rota elegível ..... 5.740.976  (99,6%)
-- ou seja, o filtro custa ~0,36% do domínio. A intuição de que instrumentos
-- reais teriam movimentações em outras rotas NÃO se confirma neste dado.
--
-- ATENÇÃO: o número acima é do LCI. O equivalente para LCA (objeto 843,
-- NUM_TIPO_IF=96) NÃO foi medido — se lá o filtro zerar o domínio, remova o
-- INNER/ANTI JOIN de ROTA_INVALIDA deste bloco e trate como divergência do
-- validador (o check do CDB, que passa, ressalva "historical operation types
-- are not constrained"; o de LCI/LCA não tem essa ressalva).
-- ===========================================================================
-- ===========================================================================
-- LASTRO (CREDITO_SCR): amarração TENTADA e REVERTIDA em 2026-08-22.
--
-- A hipótese era que o lastro da LCI fosse CREDITO_SCR.NUM_IF = LCI.NUM_IF.
-- Medido no QAB, é FALSO:
--     LCIs ativas (NUM_TIPO_IF=81) .................... 5.761.483
--     LCIs com CREDITO_SCR pelo mesmo NUM_IF ..........         0
-- O INNER JOIN zerava o domínio e o run abortava com
--     "Domínio esgotado pela admissão FK do destino: 0 instrumento(s)".
--
-- A distribuição de CREDITO_SCR (DAT_EXCLUSAO IS NULL) mostra por quê:
--     NUM_TIPO_IF   linhas      instrumentos distintos (NUM_IF)
--          143      2.468.640            0     <- o grosso, com NUM_IF NULO
--           86          1.624        1.624
--           53             55           55
--          123             10           10
--            5              7            7
--           13              4            4
-- NUM_TIPO_IF 81 (LCI) e 96 (LCA) NÃO APARECEM. E o tipo dominante (143) tem
-- NUM_IF nulo em todas as linhas. Ou seja, CREDITO_SCR.NUM_IF não é — e não
-- pode ser — o caminho da LCI até o lastro.
--
-- NÃO recoloque este join. Descobrir o vínculo real exige alguém que conheça o
-- modelo: o candidato mais provável é NUM_ID_LOTE (via LOTE), já que é a única
-- outra FK de CREDITO_SCR que não é tabela de domínio.
-- ===========================================================================
SELECT DISTINCT F.NUM_IF
FROM FLAGS_IF F
    INNER JOIN DEP_IF DEP
        ON DEP.NUM_IF = F.NUM_IF
    INNER JOIN {{RAW_INSTRUMENTO_FINANCEIRO}} IFL
        ON IFL.NUM_IF = F.NUM_IF
    INNER JOIN LOTE_ELEGIVEL LE
        ON LE.NUM_ID_LOTE = IFL.NUM_ID_LOTE
    INNER JOIN LOTE_COM_LASTRO LCL
        ON LCL.NUM_ID_LOTE = IFL.NUM_ID_LOTE
    LEFT ANTI JOIN ROTA_INVALIDA RI
        ON RI.NUM_IF = F.NUM_IF
    LEFT ANTI JOIN CARTEIRA_DUPLICADA CD
        ON CD.NUM_IF = F.NUM_IF
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
        INNER JOIN {{RAW_OPERACAO}} OPER
            ON OPER.NUM_IF = IFE.NUM_IF
    WHERE IFE.NUM_TIPO_IF = 96
        AND RES.COD_COND_RESGATE IN ('SEM TABELA')
        AND TIT.COD_TIPO_ESCALONAMENTO IS NULL
        AND IFE.DAT_EXCLUSAO IS NULL
        AND CIF.DAT_EXCLUSAO IS NULL
        AND RES.DAT_EXCLUSAO IS NULL
        AND OPER.COD_SITUACAO_OPERACAO = 43
        AND OPER.COD_CONTA_PARTE LIKE "%10-%"
        AND OPER.COD_CONTA_CONTRAPARTE LIKE "%40-%"
        AND OPER.NUM_ID_TIPO_OPER_OBJETO_SERV = 3858
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
ROTA_ELEGIVEL AS (
    -- Ver cabeçalho acima antes de alterar/remover.
    SELECT DISTINCT TOS.NUM_ID_TIPO_OPER_OBJETO_SERV
    FROM {{RAW_TIPO_OPER_OBJETO_SERV}} TOS
        INNER JOIN {{RAW_TIPO_OPERACAO}} TOP
            ON TOP.NUM_ID_TIPO_OPERACAO = TOS.NUM_ID_TIPO_OPERACAO
    WHERE TOS.NUM_ID_OBJETO_SERVICO = 843
        AND TRIM(TOP.COD_TIPO_OPERACAO) = '1'
        AND TRIM(TOS.IND_DISPONIVEL_IDENTIFICACAO) = 'S'
),
ROTA_INVALIDA AS (
    -- Basta UMA operação fora da rota elegível para reprovar o instrumento
    -- inteiro, porque o check é linha a linha sobre OPERACAO.
    SELECT DISTINCT O.NUM_IF
    FROM {{RAW_OPERACAO}} O
        LEFT JOIN ROTA_ELEGIVEL RE
            ON RE.NUM_ID_TIPO_OPER_OBJETO_SERV = O.NUM_ID_TIPO_OPER_OBJETO_SERV
    WHERE RE.NUM_ID_TIPO_OPER_OBJETO_SERV IS NULL
),
LOTE_ELEGIVEL AS (
    -- 6g.lookup.lot_root_type: INSTRUMENTO_FINANCEIRO.NUM_ID_LOTE precisa resolver para um
    -- LOTE de NUM_ID_TIPO_LOTE = 2 com conta participante.
    -- NB: o validador também exige o lote ATIVO no destino; não filtro
    -- DAT_EXCLUSAO aqui porque a coluna não está declarada no spec de LOTE.
    SELECT DISTINCT L.NUM_ID_LOTE
    FROM {{RAW_LOTE}} L
    WHERE L.NUM_ID_TIPO_LOTE = 2
        AND L.NUM_CONTA_PARTICIPANTE IS NOT NULL
),
LOTE_COM_DC AS (
    -- VINCULO DE LASTRO. O LCA sintetico preserva o NUM_ID_LOTE
    -- original (LOTE e static no fecho deste produto), entao so pode
    -- entrar instrumento cujo lote tenha CREDITO_DC ativo para semear.
    -- Sem isso a semente lateral nao tem de onde tirar linha e o
    -- sintetico nasceria sem lastro.
    -- MEDIDO no QAB: custa 3.317 de 3.763.843 LCAs (0,09%).
    SELECT DISTINCT S.NUM_ID_LOTE
    FROM {{RAW_CREDITO_DC}} S
    WHERE S.DAT_EXCLUSAO IS NULL
        AND S.NUM_ID_LOTE IS NOT NULL
),
CARTEIRA_DUPLICADA AS (
    -- 6g.wallet.*.local: a chave natural da carteira precisa ser única por
    -- instrumento. Quando a origem já tem duas linhas com a mesma chave, o
    -- clone herda a duplicidade — então o instrumento sai do domínio.
    SELECT DISTINCT NUM_IF FROM (
        SELECT CC.NUM_IF
        FROM {{RAW_CARTEIRA_COMITENTE}} CC
        GROUP BY CC.NUM_IF, CC.NUM_ID_ENTIDADE, CC.COD_TIPO_POSICAO_CARTEIRA,
                 CC.NUM_SISTEMA, CC.NUM_CONTA_PARTICIPANTE
        HAVING COUNT(*) > 1
        UNION ALL
        SELECT CP.NUM_IF
        FROM {{RAW_CARTEIRA_PARTICIPANTE}} CP
        GROUP BY CP.NUM_IF, CP.COD_TIPO_POSICAO_CARTEIRA, CP.NUM_SISTEMA,
                 CP.NUM_CONTA_PARTICIPANTE
        HAVING COUNT(*) > 1
    ) D
)
-- ===========================================================================
-- ROTA DE OPERAÇÃO (6e/6g.lookup.route).
--
-- O check exige que TODA operação sintética use rota elegível — ele roda sobre
-- `tables["OPERACAO"]` inteira, sem recorte por tipo de operação:
--     bad = operations.join(eligible_routes, "route_id", "left_anti")
-- Rota elegível = objeto de serviço 75 (LCI) / 843 (LCA) + COD_TIPO_OPERACAO='1'
-- + IND_DISPONIVEL_IDENTIFICACAO='S'.
--
-- MEDIDO no QAB (LCI): existe UMA única rota elegível, e mesmo assim
--     LCIs ativas ..................................... 5.761.483
--     LCIs com TODAS as operações em rota elegível ..... 5.740.976  (99,6%)
-- ou seja, o filtro custa ~0,36% do domínio. A intuição de que instrumentos
-- reais teriam movimentações em outras rotas NÃO se confirma neste dado.
--
-- ATENÇÃO: o número acima é do LCI. O equivalente para LCA (objeto 843,
-- NUM_TIPO_IF=96) NÃO foi medido — se lá o filtro zerar o domínio, remova o
-- INNER/ANTI JOIN de ROTA_INVALIDA deste bloco e trate como divergência do
-- validador (o check do CDB, que passa, ressalva "historical operation types
-- are not constrained"; o de LCI/LCA não tem essa ressalva).
-- ===========================================================================
-- ===========================================================================
-- DIREITO CREDITÓRIO (CREDITO_DC): amarração TENTADA e REVERTIDA em 2026-08-22.
--
-- A hipótese era que o direito creditório da LCA fosse
-- CREDITO_DC.NUM_IF = LCA.NUM_IF. Medido no QAB, é FALSO:
--     LCAs ativas (NUM_TIPO_IF=96) .................... 4.868.031
--     LCAs com CREDITO_DC pelo mesmo NUM_IF ...........         0
-- O INNER JOIN zerava o domínio e o run abortava com
--     "Domínio esgotado pela admissão FK do destino: 0 instrumento(s)".
--
-- NÃO recoloque este join sem antes descobrir por qual coluna o CREDITO_DC se
-- liga à LCA. O diagnóstico que responde isso é o que já está comentado no
-- bloco `direito_creditorio` deste arquivo: distribuição de
-- CREDITO_DC.NUM_TIPO_IF.
-- ===========================================================================
SELECT DISTINCT F.NUM_IF
FROM FLAGS_IF F
    INNER JOIN DEP_IF DEP
        ON DEP.NUM_IF = F.NUM_IF
    INNER JOIN {{RAW_INSTRUMENTO_FINANCEIRO}} IFL
        ON IFL.NUM_IF = F.NUM_IF
    INNER JOIN LOTE_ELEGIVEL LE
        ON LE.NUM_ID_LOTE = IFL.NUM_ID_LOTE
    INNER JOIN LOTE_COM_DC LCD
        ON LCD.NUM_ID_LOTE = IFL.NUM_ID_LOTE
    LEFT ANTI JOIN ROTA_INVALIDA RI
        ON RI.NUM_IF = F.NUM_IF
    LEFT ANTI JOIN CARTEIRA_DUPLICADA CD
        ON CD.NUM_IF = F.NUM_IF
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
WITH OPER_REGISTRO AS (
    -- 6h.lookup.registration_route: todo CCB ativo precisa ter AO MENOS UMA
    -- operação de registro na rota aprovada — objeto de serviço 47,
    -- COD_TIPO_OPERACAO = '1' e identificação habilitada. O check ignora as
    -- rotas históricas ("historical routes ignored"), então basta existir uma;
    -- é o mesmo desenho do 6.required.operation_tos do CDB (lá, objeto 44), e
    -- NÃO o linha-a-linha de 6e/6g.lookup.route (LCI/LCA).
    --
    -- MEDIDO no QAB: 2.227.477 CCBs (NUM_TIPO_IF=53) têm essa rota, contra
    -- lotes de 100.000 por run — folga suficiente mesmo depois de o domínio
    -- ainda ser repartido entre as 5 variantes por indexador/forma de pagamento.
    SELECT DISTINCT O.NUM_IF
    FROM {{RAW_OPERACAO}} O
        INNER JOIN {{RAW_TIPO_OPER_OBJETO_SERV}} TOS
            ON TOS.NUM_ID_TIPO_OPER_OBJETO_SERV = O.NUM_ID_TIPO_OPER_OBJETO_SERV
        INNER JOIN {{RAW_TIPO_OPERACAO}} TOP
            ON TOP.NUM_ID_TIPO_OPERACAO = TOS.NUM_ID_TIPO_OPERACAO
    WHERE TOS.NUM_ID_OBJETO_SERVICO = 47
        AND TRIM(TOP.COD_TIPO_OPERACAO) = '1'
        AND TRIM(TOS.IND_DISPONIVEL_IDENTIFICACAO) = 'S'
        AND O.COD_SITUACAO_OPERACAO = 43
        AND O.COD_CONTA_PARTE LIKE "%00-%"
        AND O.COD_CONTA_CONTRAPARTE LIKE "%40-%"
        AND O.NUM_ID_TIPO_OPER_OBJETO_SERV = 871
),
OPERACAO_INVALIDA AS (
    SELECT DISTINCT O.NUM_IF
    FROM {{RAW_OPERACAO}} O
    WHERE O.NUM_ID_TIPO_OPER_OBJETO_SERV IS NULL
        OR O.NUM_ID_TIPO_OPER_OBJETO_SERV <> 871
        OR O.COD_SITUACAO_OPERACAO IS NULL
        OR O.COD_SITUACAO_OPERACAO <> 43
)
SELECT DISTINCT I.NUM_IF
FROM {{RAW_INSTRUMENTO_FINANCEIRO}} I
    INNER JOIN {{RAW_FORMA_PAGAMENTO}} FP
        ON FP.NUM_ID_FORMA_PAGAMENTO = I.NUM_ID_FORMA_PAGAMENTO
    INNER JOIN {{RAW_ACTPCCB_CONDICAO_IF}} ACIF
        ON ACIF.NUM_IF = I.NUM_IF
    INNER JOIN OPER_REGISTRO ORG
        ON ORG.NUM_IF = I.NUM_IF
    LEFT ANTI JOIN OPERACAO_INVALIDA OI
        ON OI.NUM_IF = I.NUM_IF
WHERE I.NUM_TIPO_IF = 53
    AND I.NUM_IF_PERTENCE IS NULL
    AND I.DAT_EXCLUSAO IS NULL
    AND UPPER(TRIM(ACIF.RENT_INDEXADOR_TAXA_FLU)) = 'PREFIXADO'
    AND UPPER(TRIM(ACIF.FORMA_PAGAMENTO)) = 'PAGAMENTO DE PARCELAS';
-- END QUERY: ccb_pppre

-- BEGIN QUERY: ccb_pfpre
-- CCB PFPRE: pagamento de parcelas fixas com indexador prefixado.
WITH OPER_REGISTRO AS (
    -- 6h.lookup.registration_route: todo CCB ativo precisa ter AO MENOS UMA
    -- operação de registro na rota aprovada — objeto de serviço 47,
    -- COD_TIPO_OPERACAO = '1' e identificação habilitada. O check ignora as
    -- rotas históricas ("historical routes ignored"), então basta existir uma;
    -- é o mesmo desenho do 6.required.operation_tos do CDB (lá, objeto 44), e
    -- NÃO o linha-a-linha de 6e/6g.lookup.route (LCI/LCA).
    --
    -- MEDIDO no QAB: 2.227.477 CCBs (NUM_TIPO_IF=53) têm essa rota, contra
    -- lotes de 100.000 por run — folga suficiente mesmo depois de o domínio
    -- ainda ser repartido entre as 5 variantes por indexador/forma de pagamento.
    SELECT DISTINCT O.NUM_IF
    FROM {{RAW_OPERACAO}} O
        INNER JOIN {{RAW_TIPO_OPER_OBJETO_SERV}} TOS
            ON TOS.NUM_ID_TIPO_OPER_OBJETO_SERV = O.NUM_ID_TIPO_OPER_OBJETO_SERV
        INNER JOIN {{RAW_TIPO_OPERACAO}} TOP
            ON TOP.NUM_ID_TIPO_OPERACAO = TOS.NUM_ID_TIPO_OPERACAO
    WHERE TOS.NUM_ID_OBJETO_SERVICO = 47
        AND TRIM(TOP.COD_TIPO_OPERACAO) = '1'
        AND TRIM(TOS.IND_DISPONIVEL_IDENTIFICACAO) = 'S'
        AND O.COD_SITUACAO_OPERACAO = 43
        AND O.COD_CONTA_PARTE LIKE "%00-%"
        AND O.COD_CONTA_CONTRAPARTE LIKE "%40-%"
        AND O.NUM_ID_TIPO_OPER_OBJETO_SERV = 871
),
OPERACAO_FORA_ROTA AS (
    SELECT DISTINCT O.NUM_IF
    FROM {{RAW_OPERACAO}} O
    WHERE O.NUM_ID_TIPO_OPER_OBJETO_SERV IS NULL
        OR O.NUM_ID_TIPO_OPER_OBJETO_SERV <> 871
)
SELECT DISTINCT I.NUM_IF
FROM {{RAW_INSTRUMENTO_FINANCEIRO}} I
    INNER JOIN {{RAW_FORMA_PAGAMENTO}} FP
        ON FP.NUM_ID_FORMA_PAGAMENTO = I.NUM_ID_FORMA_PAGAMENTO
    INNER JOIN {{RAW_ACTPCCB_CONDICAO_IF}} ACIF
        ON ACIF.NUM_IF = I.NUM_IF
    INNER JOIN OPER_REGISTRO ORG
        ON ORG.NUM_IF = I.NUM_IF
    LEFT ANTI JOIN OPERACAO_FORA_ROTA OFR
        ON OFR.NUM_IF = I.NUM_IF
WHERE I.NUM_TIPO_IF = 53
    AND I.NUM_IF_PERTENCE IS NULL
    AND I.DAT_EXCLUSAO IS NULL
    AND UPPER(TRIM(ACIF.RENT_INDEXADOR_TAXA_FLU)) = 'PREFIXADO'
    AND UPPER(TRIM(ACIF.FORMA_PAGAMENTO)) = 'PAGAMENTO DE PARCELAS FIXAS';
-- END QUERY: ccb_pfpre

-- BEGIN QUERY: ccb_pgrpre
-- CCB PGRPRE: pagamento de rendimento prefixado com indexador VCP.
WITH OPER_REGISTRO AS (
    -- 6h.lookup.registration_route: todo CCB ativo precisa ter AO MENOS UMA
    -- operação de registro na rota aprovada — objeto de serviço 47,
    -- COD_TIPO_OPERACAO = '1' e identificação habilitada. O check ignora as
    -- rotas históricas ("historical routes ignored"), então basta existir uma;
    -- é o mesmo desenho do 6.required.operation_tos do CDB (lá, objeto 44), e
    -- NÃO o linha-a-linha de 6e/6g.lookup.route (LCI/LCA).
    --
    -- MEDIDO no QAB: 2.227.477 CCBs (NUM_TIPO_IF=53) têm essa rota, contra
    -- lotes de 100.000 por run — folga suficiente mesmo depois de o domínio
    -- ainda ser repartido entre as 5 variantes por indexador/forma de pagamento.
    SELECT DISTINCT O.NUM_IF
    FROM {{RAW_OPERACAO}} O
        INNER JOIN {{RAW_TIPO_OPER_OBJETO_SERV}} TOS
            ON TOS.NUM_ID_TIPO_OPER_OBJETO_SERV = O.NUM_ID_TIPO_OPER_OBJETO_SERV
        INNER JOIN {{RAW_TIPO_OPERACAO}} TOP
            ON TOP.NUM_ID_TIPO_OPERACAO = TOS.NUM_ID_TIPO_OPERACAO
    WHERE TOS.NUM_ID_OBJETO_SERVICO = 47
        AND TRIM(TOP.COD_TIPO_OPERACAO) = '1'
        AND TRIM(TOS.IND_DISPONIVEL_IDENTIFICACAO) = 'S'
        AND O.COD_SITUACAO_OPERACAO = 43
        AND O.COD_CONTA_PARTE LIKE "%00-%"
        AND O.COD_CONTA_CONTRAPARTE LIKE "%40-%"
        AND O.NUM_ID_TIPO_OPER_OBJETO_SERV = 871
)
SELECT DISTINCT I.NUM_IF
FROM {{RAW_INSTRUMENTO_FINANCEIRO}} I
    INNER JOIN {{RAW_FORMA_PAGAMENTO}} FP
        ON FP.NUM_ID_FORMA_PAGAMENTO = I.NUM_ID_FORMA_PAGAMENTO
    INNER JOIN {{RAW_ACTPCCB_CONDICAO_IF}} ACIF
        ON ACIF.NUM_IF = I.NUM_IF
    INNER JOIN OPER_REGISTRO ORG
        ON ORG.NUM_IF = I.NUM_IF
WHERE I.NUM_TIPO_IF = 53
    AND I.NUM_IF_PERTENCE IS NULL
    AND I.DAT_EXCLUSAO IS NULL
    AND UPPER(TRIM(ACIF.RENT_INDEXADOR_TAXA_FLU)) = 'VCP'
    AND UPPER(TRIM(ACIF.FORMA_PAGAMENTO)) = 'PAGAMENTO DE RENDIMENTO PREFIXADO';
-- END QUERY: ccb_pgrpre

-- BEGIN QUERY: ccb_favcp
-- CCB FAVCP: liquidação fora do âmbito B3 com indexador VCP.
WITH OPER_REGISTRO AS (
    -- 6h.lookup.registration_route: todo CCB ativo precisa ter AO MENOS UMA
    -- operação de registro na rota aprovada — objeto de serviço 47,
    -- COD_TIPO_OPERACAO = '1' e identificação habilitada. O check ignora as
    -- rotas históricas ("historical routes ignored"), então basta existir uma;
    -- é o mesmo desenho do 6.required.operation_tos do CDB (lá, objeto 44), e
    -- NÃO o linha-a-linha de 6e/6g.lookup.route (LCI/LCA).
    --
    -- MEDIDO no QAB: 2.227.477 CCBs (NUM_TIPO_IF=53) têm essa rota, contra
    -- lotes de 100.000 por run — folga suficiente mesmo depois de o domínio
    -- ainda ser repartido entre as 5 variantes por indexador/forma de pagamento.
    SELECT DISTINCT O.NUM_IF
    FROM {{RAW_OPERACAO}} O
        INNER JOIN {{RAW_TIPO_OPER_OBJETO_SERV}} TOS
            ON TOS.NUM_ID_TIPO_OPER_OBJETO_SERV = O.NUM_ID_TIPO_OPER_OBJETO_SERV
        INNER JOIN {{RAW_TIPO_OPERACAO}} TOP
            ON TOP.NUM_ID_TIPO_OPERACAO = TOS.NUM_ID_TIPO_OPERACAO
    WHERE TOS.NUM_ID_OBJETO_SERVICO = 47
        AND TRIM(TOP.COD_TIPO_OPERACAO) = '1'
        AND TRIM(TOS.IND_DISPONIVEL_IDENTIFICACAO) = 'S'
        AND O.COD_SITUACAO_OPERACAO = 43
        AND O.COD_CONTA_PARTE LIKE "%00-%"
        AND O.COD_CONTA_CONTRAPARTE LIKE "%40-%"
        AND O.NUM_ID_TIPO_OPER_OBJETO_SERV = 871
),
OPERACAO_FORA_ROTA AS (
    SELECT DISTINCT O.NUM_IF
    FROM {{RAW_OPERACAO}} O
    WHERE O.NUM_ID_TIPO_OPER_OBJETO_SERV IS NULL
        OR O.NUM_ID_TIPO_OPER_OBJETO_SERV <> 871
)
SELECT DISTINCT I.NUM_IF
FROM {{RAW_INSTRUMENTO_FINANCEIRO}} I
    INNER JOIN {{RAW_FORMA_PAGAMENTO}} FP
        ON FP.NUM_ID_FORMA_PAGAMENTO = I.NUM_ID_FORMA_PAGAMENTO
    INNER JOIN {{RAW_ACTPCCB_CONDICAO_IF}} ACIF
        ON ACIF.NUM_IF = I.NUM_IF
    INNER JOIN OPER_REGISTRO ORG
        ON ORG.NUM_IF = I.NUM_IF
    LEFT ANTI JOIN OPERACAO_FORA_ROTA OFR
        ON OFR.NUM_IF = I.NUM_IF
WHERE I.NUM_TIPO_IF = 53
    AND I.NUM_IF_PERTENCE IS NULL
    AND I.DAT_EXCLUSAO IS NULL
    AND UPPER(TRIM(ACIF.RENT_INDEXADOR_TAXA_FLU)) = 'VCP'
    AND UPPER(TRIM(ACIF.FORMA_PAGAMENTO)) = 'LIQUIDAÇÃO FORA DO ÂMBITO B3';
-- END QUERY: ccb_favcp

-- BEGIN QUERY: ccb_fapre
-- CCB FAPRE: liquidação fora do âmbito B3 com indexador prefixado.
WITH OPER_REGISTRO AS (
    -- 6h.lookup.registration_route: todo CCB ativo precisa ter AO MENOS UMA
    -- operação de registro na rota aprovada — objeto de serviço 47,
    -- COD_TIPO_OPERACAO = '1' e identificação habilitada. O check ignora as
    -- rotas históricas ("historical routes ignored"), então basta existir uma;
    -- é o mesmo desenho do 6.required.operation_tos do CDB (lá, objeto 44), e
    -- NÃO o linha-a-linha de 6e/6g.lookup.route (LCI/LCA).
    --
    -- MEDIDO no QAB: 2.227.477 CCBs (NUM_TIPO_IF=53) têm essa rota, contra
    -- lotes de 100.000 por run — folga suficiente mesmo depois de o domínio
    -- ainda ser repartido entre as 5 variantes por indexador/forma de pagamento.
    SELECT DISTINCT O.NUM_IF
    FROM {{RAW_OPERACAO}} O
        INNER JOIN {{RAW_TIPO_OPER_OBJETO_SERV}} TOS
            ON TOS.NUM_ID_TIPO_OPER_OBJETO_SERV = O.NUM_ID_TIPO_OPER_OBJETO_SERV
        INNER JOIN {{RAW_TIPO_OPERACAO}} TOP
            ON TOP.NUM_ID_TIPO_OPERACAO = TOS.NUM_ID_TIPO_OPERACAO
    WHERE TOS.NUM_ID_OBJETO_SERVICO = 47
        AND TRIM(TOP.COD_TIPO_OPERACAO) = '1'
        AND TRIM(TOS.IND_DISPONIVEL_IDENTIFICACAO) = 'S'
        AND O.COD_SITUACAO_OPERACAO = 43
        AND O.COD_CONTA_PARTE LIKE "%00-%"
        AND O.COD_CONTA_CONTRAPARTE LIKE "%40-%"
        AND O.NUM_ID_TIPO_OPER_OBJETO_SERV = 871
)
SELECT DISTINCT I.NUM_IF
FROM {{RAW_INSTRUMENTO_FINANCEIRO}} I
    INNER JOIN {{RAW_FORMA_PAGAMENTO}} FP
        ON FP.NUM_ID_FORMA_PAGAMENTO = I.NUM_ID_FORMA_PAGAMENTO
    INNER JOIN {{RAW_ACTPCCB_CONDICAO_IF}} ACIF
        ON ACIF.NUM_IF = I.NUM_IF
    INNER JOIN OPER_REGISTRO ORG
        ON ORG.NUM_IF = I.NUM_IF
WHERE I.NUM_TIPO_IF = 53
    AND I.NUM_IF_PERTENCE IS NULL
    AND I.DAT_EXCLUSAO IS NULL
    AND UPPER(TRIM(ACIF.RENT_INDEXADOR_TAXA_FLU)) = 'PREFIXADO'
    AND UPPER(TRIM(ACIF.FORMA_PAGAMENTO)) = 'LIQUIDAÇÃO FORA DO ÂMBITO B3';
-- END QUERY: ccb_fapre

-- BEGIN QUERY: gravame
-- Query Spark SQL que define o domínio de GRAVAMEs a clonar.
-- Contrato: retornar somente uma coluna chamada NUM_IF, sem valores nulos.
-- filtro num_tipo_if 175 (GRVM), ativo e não cancelado.
--
-- POR QUE NÃO HÁ JOIN COM IF_GRVM / COMPLEMENTO_CONTRATO / PARAMETRO_PONTA /
-- ARQUIVO_IF / PROTOCOLO / DADO_OPERACAO / LANCAMENTO:
-- as checagens 2i.*.edge são de ÓRFÃO, não de cobertura —
--     bad = children.join(parent_ids, "parent_id", "left_anti")
-- ou seja, exigem que todo FILHO presente tenha pai no fecho, e não que todo
-- pai tenha filho. Como o fecho desce da raiz, isso já vale por construção.
-- Exigir a presença dos filhos aqui é sobre-restrição: medido no QAB, o
-- predicado "tem operação com DADO_OPERACAO E LANCAMENTO" dá ZERO gravames e
-- zeraria o domínio inteiro.
--
-- O ÚNICO filtro obrigatório é a rota de registro (6i.lookup.registration_route,
-- "Every active Gravame has an approved registration route"), que é do tipo
-- "existe ao menos uma" por raiz.
--
-- ATENÇÃO — a rota do GRAVAME é diferente dos demais produtos:
--   objeto de serviço 1132 + COD_TIPO_OPERACAO = '520'  (não '1')
--   e o check NÃO confere IND_DISPONIVEL_IDENTIFICACAO.
-- Comparar: CDB 44/'1', CCB 47/'1', LCI 75/'1', LCA 843/'1'.
--
-- MEDIDO no QAB:
--   existe exatamente UMA rota assim: NUM_ID_TIPO_OPER_OBJETO_SERV = 15394
--   (é uma das rotas "root-side" que o próprio validador lista em
--    2i.operation_route_membership: 15394 e 15512);
--   gravames ativos e não cancelados ............. 9.417.279
--   com essa rota de registro ....................     2.403   <- TETO DO DOMÍNIO
-- Ou seja, o domínio do produto é ~2.4 mil instrumentos. Para volume maior,
-- conte com o ajuste automático de K (n × K é preservado).
WITH FILTRO_BASE AS (
    SELECT DISTINCT IFE.NUM_IF
    FROM {{RAW_INSTRUMENTO_FINANCEIRO}} IFE
        INNER JOIN {{RAW_COMPLEMENTO_CONTRATO}} CT
            ON CT.NUM_IF = IFE.NUM_IF
        INNER JOIN {{RAW_OPERACAO}} OPER
            ON OPER.NUM_IF = IFE.NUM_IF
    WHERE IFE.NUM_TIPO_IF = 175
        AND IFE.DAT_EXCLUSAO IS NULL
        AND CT.DAT_EXCLUSAO IS NULL
        AND OPER.COD_SITUACAO_OPERACAO = 43
        AND OPER.COD_CONTA_PARTE LIKE "%00-%"
        AND OPER.COD_CONTA_CONTRAPARTE LIKE "%10-%"
        AND OPER.NUM_ID_TIPO_OPER_OBJETO_SERV IN (15394, 15512)
),
OPER_REGISTRO AS (
    SELECT DISTINCT O.NUM_IF
    FROM {{RAW_OPERACAO}} O
        INNER JOIN {{RAW_TIPO_OPER_OBJETO_SERV}} TOS
            ON TOS.NUM_ID_TIPO_OPER_OBJETO_SERV = O.NUM_ID_TIPO_OPER_OBJETO_SERV
        INNER JOIN {{RAW_TIPO_OPERACAO}} TOP
            ON TOP.NUM_ID_TIPO_OPERACAO = TOS.NUM_ID_TIPO_OPERACAO
    WHERE TOS.NUM_ID_OBJETO_SERVICO = 1132
        AND TRIM(TOP.COD_TIPO_OPERACAO) IN ('520', '527')
),
OPERACAO_FORA_ROTA AS (
    SELECT DISTINCT O.NUM_IF
    FROM {{RAW_OPERACAO}} O
    WHERE O.NUM_ID_TIPO_OPER_OBJETO_SERV IS NULL
        OR O.NUM_ID_TIPO_OPER_OBJETO_SERV NOT IN (15394, 15512)
)
SELECT DISTINCT F.NUM_IF
FROM FILTRO_BASE F
    INNER JOIN OPER_REGISTRO ORG
        ON ORG.NUM_IF = F.NUM_IF
    LEFT ANTI JOIN OPERACAO_FORA_ROTA OFR
        ON OFR.NUM_IF = F.NUM_IF;
-- END QUERY: gravame

-- BEGIN QUERY: lastro
-- Domínio de instrumentos com crédito SCR ativo.
-- Contrato: retornar somente uma coluna chamada NUM_IF, sem valores nulos.
-- Diagnóstico separado dos tipos presentes:
-- SELECT DISTINCT SCR.NUM_TIPO_IF
-- FROM {{RAW_CREDITO_SCR}} SCR
-- WHERE SCR.DAT_EXCLUSAO IS NULL
--     AND SCR.NUM_TIPO_IF IS NOT NULL;
SELECT DISTINCT SCR.NUM_IF
FROM {{RAW_CREDITO_SCR}} SCR
WHERE SCR.DAT_EXCLUSAO IS NULL
    AND SCR.NUM_IF IS NOT NULL;
-- END QUERY: lastro

-- BEGIN QUERY: direito_creditorio
-- Domínio de instrumentos com direito creditório ativo.
-- Contrato: retornar somente uma coluna chamada NUM_IF, sem valores nulos.
-- Diagnóstico separado dos tipos presentes:
-- SELECT DISTINCT CDC.NUM_TIPO_IF
-- FROM {{RAW_CREDITO_DC}} CDC
-- WHERE CDC.DAT_EXCLUSAO IS NULL
--     AND CDC.NUM_TIPO_IF IS NOT NULL;
SELECT DISTINCT CDC.NUM_IF
FROM {{RAW_CREDITO_DC}} CDC
WHERE CDC.DAT_EXCLUSAO IS NULL
    AND CDC.NUM_IF IS NOT NULL;
-- END QUERY: direito_creditorio
