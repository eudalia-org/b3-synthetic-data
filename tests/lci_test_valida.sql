Bloco A — pré-requisitos: matam ou liberam a Opção 1
Q1 — NUM_ID_LOTE está preenchido nas LCIs/LCAs?
Se for majoritariamente nulo, a Opção 1 morre aqui e não precisa rodar o resto.


SELECT I.NUM_TIPO_IF,
       COUNT(*)                                                   AS IFS_ATIVOS,
       COUNT(I.NUM_ID_LOTE)                                       AS COM_LOTE,
       COUNT(*) - COUNT(I.NUM_ID_LOTE)                            AS SEM_LOTE,
       ROUND(100 * COUNT(I.NUM_ID_LOTE) / NULLIF(COUNT(*),0), 2)  AS PCT_COM_LOTE,
       COUNT(DISTINCT I.NUM_ID_LOTE)                              AS LOTES_DISTINTOS
FROM INSTRUMENTO_FINANCEIRO I
WHERE I.NUM_TIPO_IF IN (81, 96)
  AND I.DAT_EXCLUSAO IS NULL
GROUP BY I.NUM_TIPO_IF;
Critério: PCT_COM_LOTE alto (>90%). Se ~0 → Opção 1 inviável, vá direto para a Opção 2.

Q2 — ⚠️ o teste que mais pode matar: crédito vive em lote do tipo certo?
LCI só aceita lote tipo 1. Se CREDITO_SCR só existe em lotes de outro tipo, nenhuma LCI elegível terá lastro.


SELECT L.NUM_ID_TIPO_LOTE,
       COUNT(DISTINCT S.NUM_ID_LOTE) AS LOTES_COM_SCR,
       COUNT(*)                      AS LINHAS_SCR
FROM CREDITO_SCR S
    INNER JOIN LOTE L ON L.NUM_ID_LOTE = S.NUM_ID_LOTE
WHERE S.DAT_EXCLUSAO IS NULL
GROUP BY L.NUM_ID_TIPO_LOTE
ORDER BY 3 DESC;

-- direito creditório (LCA espera tipo 2)
SELECT L.NUM_ID_TIPO_LOTE,
       COUNT(DISTINCT D.NUM_ID_LOTE) AS LOTES_COM_DC,
       COUNT(*)                      AS LINHAS_DC
FROM CREDITO_DC D
    INNER JOIN LOTE L ON L.NUM_ID_LOTE = D.NUM_ID_LOTE
WHERE D.DAT_EXCLUSAO IS NULL
GROUP BY L.NUM_ID_TIPO_LOTE
ORDER BY 3 DESC;
Critério: precisa aparecer NUM_ID_TIPO_LOTE = 1 na primeira e = 2 na segunda, com contagem relevante. Se não aparecer → Opção 1 está morta e nenhuma query adiante importa.

Q3 — quantas LCIs/LCAs ativas já têm lote com lastro (sem os outros filtros)

SELECT
  (SELECT COUNT(*) FROM INSTRUMENTO_FINANCEIRO I
    WHERE I.NUM_TIPO_IF = 81 AND I.DAT_EXCLUSAO IS NULL)            AS LCI_ATIVAS,
  (SELECT COUNT(*) FROM INSTRUMENTO_FINANCEIRO I
    WHERE I.NUM_TIPO_IF = 81 AND I.DAT_EXCLUSAO IS NULL
      AND EXISTS (SELECT 1 FROM CREDITO_SCR S
                   WHERE S.NUM_ID_LOTE = I.NUM_ID_LOTE
                     AND S.DAT_EXCLUSAO IS NULL))                   AS LCI_COM_LASTRO,
  (SELECT COUNT(*) FROM INSTRUMENTO_FINANCEIRO I
    WHERE I.NUM_TIPO_IF = 96 AND I.DAT_EXCLUSAO IS NULL)            AS LCA_ATIVAS,
  (SELECT COUNT(*) FROM INSTRUMENTO_FINANCEIRO I
    WHERE I.NUM_TIPO_IF = 96 AND I.DAT_EXCLUSAO IS NULL
      AND EXISTS (SELECT 1 FROM CREDITO_DC D
                   WHERE D.NUM_ID_LOTE = I.NUM_ID_LOTE
                     AND D.DAT_EXCLUSAO IS NULL))                   AS LCA_COM_DC
FROM DUAL;
Referência conhecida: 5.761.483 LCIs e 4.868.031 LCAs ativas.

Bloco B — o número que decide: funil completo do LCI
Este é o único número que realmente importa, porque combina a condição nova com todos os filtros que já existem hoje.

Q4 — LCI: domínio de hoje vs. domínio com lastro

WITH FILTRO_BASE AS (
    SELECT DISTINCT IFE.NUM_IF, IFE.NUM_ID_LOTE
    FROM INSTRUMENTO_FINANCEIRO IFE
        INNER JOIN TITULO      TIT ON TIT.NUM_IF = IFE.NUM_IF
        INNER JOIN CONDICAO_IF CIF ON CIF.NUM_IF = IFE.NUM_IF
        INNER JOIN RESGATE     RES ON RES.NUM_CONDICAO_IF = CIF.NUM_CONDICAO_IF
    WHERE IFE.NUM_TIPO_IF = 81
      AND RES.COD_COND_RESGATE = 'SEM TABELA'
      AND TIT.COD_TIPO_ESCALONAMENTO IS NULL
      AND IFE.DAT_EXCLUSAO IS NULL
      AND CIF.DAT_EXCLUSAO IS NULL
      AND RES.DAT_EXCLUSAO IS NULL
),
ROTA_ELEGIVEL AS (
    SELECT DISTINCT TOS.NUM_ID_TIPO_OPER_OBJETO_SERV
    FROM TIPO_OPER_OBJETO_SERV TOS
        INNER JOIN TIPO_OPERACAO TOP2
            ON TOP2.NUM_ID_TIPO_OPERACAO = TOS.NUM_ID_TIPO_OPERACAO
    WHERE TOS.NUM_ID_OBJETO_SERVICO = 75
      AND TRIM(TOP2.COD_TIPO_OPERACAO) = '1'
      AND TRIM(TOS.IND_DISPONIVEL_IDENTIFICACAO) = 'S'
),
ROTA_INVALIDA AS (
    SELECT DISTINCT O.NUM_IF
    FROM OPERACAO O
    WHERE NOT EXISTS (SELECT 1 FROM ROTA_ELEGIVEL RE
                       WHERE RE.NUM_ID_TIPO_OPER_OBJETO_SERV
                           = O.NUM_ID_TIPO_OPER_OBJETO_SERV)
),
CARTEIRA_DUPLICADA AS (
    SELECT DISTINCT NUM_IF FROM (
        SELECT CC.NUM_IF FROM CARTEIRA_COMITENTE CC
        GROUP BY CC.NUM_IF, CC.NUM_ID_ENTIDADE, CC.COD_TIPO_POSICAO_CARTEIRA,
                 CC.NUM_SISTEMA, CC.NUM_CONTA_PARTICIPANTE
        HAVING COUNT(*) > 1
        UNION ALL
        SELECT CP.NUM_IF FROM CARTEIRA_PARTICIPANTE CP
        GROUP BY CP.NUM_IF, CP.COD_TIPO_POSICAO_CARTEIRA, CP.NUM_SISTEMA,
                 CP.NUM_CONTA_PARTICIPANTE
        HAVING COUNT(*) > 1
    ) D
),
DOMINIO_HOJE AS (
    SELECT FB.NUM_IF, FB.NUM_ID_LOTE
    FROM FILTRO_BASE FB
    WHERE EXISTS (SELECT 1 FROM CONDICAO_IF C
                   WHERE C.NUM_IF = FB.NUM_IF AND C.DAT_EXCLUSAO IS NULL
                     AND C.COD_TIPO_CONDICAO_IF <> 20)
      AND EXISTS (SELECT 1 FROM DEPOSITO_AUTOMATICO_IF DP
                   WHERE DP.NUM_IF = FB.NUM_IF)
      AND EXISTS (SELECT 1 FROM LOTE L
                   WHERE L.NUM_ID_LOTE = FB.NUM_ID_LOTE
                     AND L.NUM_ID_TIPO_LOTE = 1
                     AND L.NUM_CONTA_PARTICIPANTE IS NOT NULL)
      AND NOT EXISTS (SELECT 1 FROM ROTA_INVALIDA RI WHERE RI.NUM_IF = FB.NUM_IF)
      AND NOT EXISTS (SELECT 1 FROM CARTEIRA_DUPLICADA CD WHERE CD.NUM_IF = FB.NUM_IF)
      AND EXISTS (
            SELECT 1 FROM OPERACAO O
            WHERE O.NUM_IF = FB.NUM_IF
              AND EXISTS (SELECT 1 FROM DADO_OPERACAO DOP
                           WHERE DOP.NUM_ID_OPERACAO = O.NUM_ID_OPERACAO)
              AND EXISTS (SELECT 1 FROM LANCAMENTO LAN
                           WHERE LAN.NUM_ID_OPERACAO = O.NUM_ID_OPERACAO)
              AND EXISTS (SELECT 1 FROM ESPECIFICACAO ESP
                           WHERE ESP.NUM_ID_OPERACAO = O.NUM_ID_OPERACAO
                             AND EXISTS (SELECT 1 FROM ESPECIFICACAO_COMITENTE EPC
                                          WHERE EPC.NUM_ID_ESPECIFICACAO
                                              = ESP.NUM_ID_ESPECIFICACAO)))
)
SELECT COUNT(*)                                AS DOMINIO_HOJE,
       COUNT(CASE WHEN EXISTS (
             SELECT 1 FROM CREDITO_SCR S
              WHERE S.NUM_ID_LOTE = DH.NUM_ID_LOTE
                AND S.DAT_EXCLUSAO IS NULL) THEN 1 END)  AS DOMINIO_COM_LASTRO,
       ROUND(100 * COUNT(CASE WHEN EXISTS (
             SELECT 1 FROM CREDITO_SCR S
              WHERE S.NUM_ID_LOTE = DH.NUM_ID_LOTE
                AND S.DAT_EXCLUSAO IS NULL) THEN 1 END)
             / NULLIF(COUNT(*),0), 2)                    AS PCT_SOBREVIVE
FROM DOMINIO_HOJE DH;
Q5 — LCA: mesma query, três trocas
NUM_TIPO_IF = 96, NUM_ID_OBJETO_SERVICO = 843, NUM_ID_TIPO_LOTE = 2, e CREDITO_SCR→CREDITO_DC.

Critério de decisão:

DOMINIO_COM_LASTRO	leitura
≥ 100× o N que você amostra	✅ Opção 1 é a melhor. Implementar.
entre 1× e 100× N	⚠️ funciona, mas amostra pobre — cheque a Q6
0	❌ Opção 1 morta → Opção 2
Bloco C — qualidade da amostra
Q6 — concentração: o domínio sobrevivente é diverso?
Domínio grande concentrado em 3 lotes gera sintético repetitivo e pode furar 6e.wallet.*.


SELECT COUNT(DISTINCT I.NUM_ID_LOTE)                              AS LOTES_DISTINTOS,
       COUNT(*)                                                   AS IFS,
       ROUND(COUNT(*) / NULLIF(COUNT(DISTINCT I.NUM_ID_LOTE),0),2) AS IFS_POR_LOTE
FROM INSTRUMENTO_FINANCEIRO I
WHERE I.NUM_TIPO_IF = 81
  AND I.DAT_EXCLUSAO IS NULL
  AND EXISTS (SELECT 1 FROM LOTE L
               WHERE L.NUM_ID_LOTE = I.NUM_ID_LOTE
                 AND L.NUM_ID_TIPO_LOTE = 1
                 AND L.NUM_CONTA_PARTICIPANTE IS NOT NULL)
  AND EXISTS (SELECT 1 FROM CREDITO_SCR S
               WHERE S.NUM_ID_LOTE = I.NUM_ID_LOTE
                 AND S.DAT_EXCLUSAO IS NULL);
Critério: LOTES_DISTINTOS confortavelmente acima do N amostrado.

Bloco D — comparar com a Opção 2
Q7 — quantos créditos por lote? (custo real da Opção 2)

SELECT MIN(C) AS MIN_CRED, ROUND(AVG(C),1) AS AVG_CRED,
       MAX(C) AS MAX_CRED, MEDIAN(C) AS MEDIANA,
       SUM(CASE WHEN C > 1000 THEN 1 ELSE 0 END) AS LOTES_ACIMA_1000
FROM (SELECT S.NUM_ID_LOTE, COUNT(*) C
      FROM CREDITO_SCR S
      WHERE S.DAT_EXCLUSAO IS NULL
      GROUP BY S.NUM_ID_LOTE);
Interpretação: MAX_CRED alto (dezenas de milhares) significa que a semente lateral da Opção 2 arrastaria volume enorme num run de 100 LCIs — o que reforça a Opção 1. Essa query não valida a Opção 1; ela prova que a alternativa é pior.

Bloco E — no destino (QAB), não na origem
Q8 — o destino tem lastro real para a LCI apontar?
É a premissa central da Opção 1: o vínculo aponta para dado real já carregado.


SELECT (SELECT COUNT(*) FROM LOTE)        AS LOTES_NO_DESTINO,
       (SELECT COUNT(*) FROM CREDITO_SCR) AS CREDITO_SCR_NO_DESTINO,
       (SELECT COUNT(*) FROM CREDITO_DC)  AS CREDITO_DC_NO_DESTINO
FROM DUAL;
Critério: se CREDITO_SCR_NO_DESTINO = 0, a LCI sintética apontaria para um lote cujo lastro não existe no destino. Nesse caso a Opção 1 satisfaz a regra na origem mas não no destino → Opção 2 vira obrigatória.

Ordem de execução e atalhos
Q2 primeiro. É a mais barata e a que mais mata. Sem tipo 1 / tipo 2, pare.
Q1 — nulidade do lote.
Q4 e Q5 — o número que decide.
Q8 — decide entre "só query" e "precisa gerar lastro".
Q3, Q6, Q7 — contexto e comparação.
A conclusão sai do cruzamento de Q4 com Q8: domínio sobrevivente robusto e destino com CREDITO_SCR populado ⇒ Opção 1 é a melhor, e implemento com ~8 linhas de SQL por bloco, sem tocar no motor. Qualquer um dos dois falhando ⇒ Opção 2, com o teto por lote que a Q7 dimensiona.

Me mande os resultados que eu fecho a recomendação.
