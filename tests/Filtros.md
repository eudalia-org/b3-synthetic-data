## 1. Predicados de fonte por tabela (`FILTROS_FONTE`)
 
Aplicados na **leitura de cada Parquet**, antes de qualquer amostragem ou propagação. Toda leitura de fonte passa por `_read_source`, então nenhuma etapa posterior consegue re-injetar uma linha removida por estes filtros.
 
| Tabela | Filtro (predicados em AND) |
|---|---|
| `INSTRUMENTO_FINANCEIRO` | `NUM_TIPO_IF = 49` **e** `DAT_EXCLUSAO IS NULL` |
| `RESGATE` | `UPPER(TRIM(COD_COND_RESGATE)) = 'SEM TABELA'` **e** `DAT_EXCLUSAO IS NULL` |
| `TITULO` | `COD_TIPO_ESCALONAMENTO IS NULL` |
| `CONDICAO_IF` | `DAT_EXCLUSAO IS NULL` |
| `CARTEIRA_COMITENTE` | `QTD_CARTEIRA_COMITENTE > 0` (linha com valor NULL também sai) |
| `CARTEIRA_PARTICIPANTE` | `QTD_CARTEIRA_PARTICIPANTE > 0` (linha com valor NULL também sai) |
 
Observações:
- Predicado cuja coluna não exista no schema da tabela é **ignorado** (defensivo contra variação de schema), com warning no log.
- A comparação de `COD_COND_RESGATE` é case/space-insensitive (`upper + trim`).
