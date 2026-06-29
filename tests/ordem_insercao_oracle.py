#!/usr/bin/env python3
"""
ordem_insercao_oracle.py

Descobre a ordem CORRETA de inserção no Oracle a partir do dicionário de
specs (PK/FK). Regra fundamental: um pai precisa estar inserido antes de
qualquer filho que o referencie via FK, senão o INSERT do filho viola a
constraint (ORA-02291: parent key not found).

Critérios tratados explicitamente (e por que importam neste specs):

1. SELF-REFERENCE (ex.: INSTRUMENTO_FINANCEIRO.NUM_IF_ORIGEM -> ela mesma):
   uma tabela não depende de si mesma para fins de ORDEM — ela é inserida
   de uma vez e a auto-FK é resolvida na própria carga (as colunas auto-ref
   deste specs são nullable, então podem entrar NULL e ser atualizadas depois,
   ou já vir consistentes no mesmo lote). Ignoramos a aresta para o toposort.

2. CICLO ENTRE TABELAS (ex.: EVENTO -> CONDICAO_IF e OPERACAO -> EVENTO, com
   EVENTO/OPERACAO se cruzando): não existe ordem perfeita dentro de um ciclo.
   O código DETECTA e REPORTA o ciclo em vez de devolver uma ordem silenciosa
   e potencialmente errada. Dentro do ciclo, quebra pela aresta que aponta para
   a FK mais provavelmente nullable, deixando claro qual FK precisará de carga
   em duas fases (INSERT com a FK do ciclo NULL -> UPDATE depois).

3. TABELAS STATIC (domínio/lookup): entram PRIMEIRO. Quase toda tabela
   transacional depende delas; além disso, no contexto de engorda elas não
   são sintetizadas (já existem no banco), mas a ordem as coloca no topo para
   o caso de uma carga completa.

4. ESCOPO: por padrão ordena TODAS as tabelas do specs. Passe `apenas=[...]`
   para restringir às 15 do CDB — as FKs que apontam para tabelas fora do
   subconjunto são ignoradas para ordem (são static já presentes no banco).

Saída: lista ordenada (pai -> filho) + relatório de self-refs e ciclos.
"""

from __future__ import annotations

from typing import Dict, List, Mapping, Optional, Set, Tuple


def _fk_list(cfg: Mapping) -> List[dict]:
    fks = cfg.get("foreign_keys")
    if not isinstance(fks, (list, tuple)):
        fks = cfg.get("fks")
    return [fk for fk in (fks or []) if isinstance(fk, dict)]


def _build_dependencies(
    specs: Mapping[str, Mapping],
    escopo: Set[str],
) -> Tuple[Dict[str, Set[str]], List[str], List[Tuple[str, str]]]:
    """
    Constrói deps[t] = conjunto de tabelas que PRECISAM vir antes de t.

    Retorna (deps, self_refs, fora_de_escopo):
      - deps: dependências válidas (pai dentro do escopo, != t).
      - self_refs: tabelas com FK para si mesmas (reportadas, não bloqueiam ordem).
      - fora_de_escopo: arestas (filho, pai) ignoradas porque o pai está fora
        do escopo (ex.: FK para tabela static não incluída) — relatadas para
        você confirmar que esses pais já existem no banco.
    """
    deps: Dict[str, Set[str]] = {t: set() for t in escopo}
    self_refs: List[str] = []
    fora_de_escopo: List[Tuple[str, str]] = []

    for table in escopo:
        cfg = specs[table]
        for fk in _fk_list(cfg):
            parent = fk.get("parent_table")
            if not parent:
                continue
            if parent == table:
                if table not in self_refs:
                    self_refs.append(table)
                continue  # self-ref não entra na ordem
            if parent not in escopo:
                fora_de_escopo.append((table, parent))
                continue  # pai fora do escopo: assume-se já presente no banco
            deps[table].add(parent)

    return deps, self_refs, fora_de_escopo


def _detect_cycles(deps: Mapping[str, Set[str]]) -> List[List[str]]:
    """
    Detecta ciclos no grafo de dependências via DFS (cores branco/cinza/preto).

    Retorna lista de ciclos; cada ciclo é a lista de nós no caminho de volta.
    Um grafo sem ciclos retorna [].
    """
    WHITE, GRAY, BLACK = 0, 1, 2
    color: Dict[str, int] = {t: WHITE for t in deps}
    stack: List[str] = []
    cycles: List[List[str]] = []

    def dfs(node: str) -> None:
        color[node] = GRAY
        stack.append(node)
        for dep in sorted(deps[node]):
            if color[dep] == GRAY:
                # achou aresta de retorno: extrai o ciclo do stack
                idx = stack.index(dep)
                cycles.append(stack[idx:] + [dep])
            elif color[dep] == WHITE:
                dfs(dep)
        stack.pop()
        color[node] = BLACK

    for t in sorted(deps):
        if color[t] == WHITE:
            dfs(t)

    return cycles


def _toposort(
    deps: Dict[str, Set[str]],
    static_tables: Set[str],
) -> Tuple[List[str], List[Tuple[str, List[str]]]]:
    """
    Kahn's algorithm com dois critérios de desempate DETERMINÍSTICOS:
      1. tabelas static primeiro (entram antes das transacionais);
      2. ordem alfabética, para reprodutibilidade.

    Se sobrar nó com dependência não resolvível (ciclo), quebra forçando o nó
    com MENOS dependências pendentes (e registra a quebra para o relatório).

    Retorna (ordem, quebras): `quebras` lista (tabela_forçada, deps_pendentes)
    para cada vez que um ciclo precisou ser quebrado.
    """
    deps = {t: set(d) for t, d in deps.items()}  # cópia mutável
    done: Set[str] = set()
    order: List[str] = []
    quebras: List[Tuple[str, List[str]]] = []

    def sort_key(t: str) -> Tuple[int, str]:
        # static primeiro (0 antes de 1), depois alfabético
        return (0 if t in static_tables else 1, t)

    remaining = set(deps)
    while remaining:
        ready = sorted(
            (t for t in remaining if deps[t] <= done),
            key=sort_key,
        )
        if not ready:
            # ciclo: força o nó com menos pendências (desempate static + nome)
            forced = min(
                remaining,
                key=lambda t: (len(deps[t] - done), sort_key(t)),
            )
            quebras.append((forced, sorted(deps[forced] - done)))
            ready = [forced]

        for t in ready:
            order.append(t)
            done.add(t)
            remaining.discard(t)

    return order, quebras


def ordem_insercao(
    specs: Mapping[str, Mapping],
    apenas: Optional[List[str]] = None,
) -> Dict[str, object]:
    """
    Calcula a ordem de inserção e devolve um relatório completo.

    apenas: se informado, ordena só essas tabelas (ex.: as 15 do CDB);
            FKs para tabelas fora da lista são ignoradas para ordem e
            reportadas em `fora_de_escopo`.
    """
    escopo: Set[str] = set(apenas) if apenas else set(specs)

    faltando = escopo - set(specs)
    if faltando:
        raise ValueError(f"Tabelas em `apenas` ausentes no specs: {sorted(faltando)}")

    static_tables = {t for t in escopo if specs[t].get("static")}

    deps, self_refs, fora_de_escopo = _build_dependencies(specs, escopo)
    cycles = _detect_cycles(deps)
    order, quebras = _toposort(deps, static_tables)

    return {
        "ordem": order,
        "self_refs": self_refs,
        "ciclos": cycles,
        "quebras_de_ciclo": quebras,
        "fora_de_escopo": fora_de_escopo,
        "static": sorted(static_tables),
    }


def imprime_relatorio(rel: Dict[str, object]) -> None:
    print("=" * 70)
    print("ORDEM DE INSERÇÃO (pai -> filho; insira de cima para baixo)")
    print("=" * 70)
    for i, t in enumerate(rel["ordem"], 1):
        marca = "  [static]" if t in set(rel["static"]) else ""
        print(f"  {i:>3}. {t}{marca}")

    if rel["self_refs"]:
        print("\n" + "-" * 70)
        print("SELF-REFERENCES (auto-FK; carregue a tabela e resolva a auto-FK")
        print("na própria carga — nullable, pode entrar NULL e ser atualizada):")
        for t in rel["self_refs"]:
            print(f"  - {t}")

    if rel["ciclos"]:
        print("\n" + "-" * 70)
        print("CICLOS DETECTADOS (não há ordem perfeita; carga em 2 fases:")
        print("INSERT com a FK do ciclo NULL -> UPDATE da FK depois):")
        for c in rel["ciclos"]:
            print("  - " + " -> ".join(c))

    if rel["quebras_de_ciclo"]:
        print("\n" + "-" * 70)
        print("QUEBRAS FORÇADAS (tabela inserida antes de um pai do ciclo;")
        print("a FK abaixo precisa ser NULL no INSERT e preenchida depois):")
        for tabela, pendentes in rel["quebras_de_ciclo"]:
            print(f"  - {tabela}  (pais ainda não inseridos: {pendentes})")

    if rel["fora_de_escopo"]:
        print("\n" + "-" * 70)
        print("FKs PARA TABELAS FORA DO ESCOPO (assumidas já presentes no banco;")
        print("confirme que existem antes da carga):")
        for filho, pai in rel["fora_de_escopo"]:
            print(f"  - {filho}.FK -> {pai}")

    print("=" * 70)


if __name__ == "__main__":
    import json
    import sys

    # Carrega o specs de um JSON passado como argumento, ou cole o dict abaixo.
    if len(sys.argv) > 1:
        with open(sys.argv[1], "r", encoding="utf-8") as f:
            SPECS = json.load(f)
    else:
        raise SystemExit(
            "Uso: python ordem_insercao_oracle.py specs.json\n"
            "  (ou importe `ordem_insercao` e passe o dict diretamente)"
        )

    # Por padrão, ordena TODAS as tabelas do specs — necessário quando as 47
    # foram sintetizadas e serão carregadas juntas (as 15 referenciam static
    # que também entram no banco; ordenar só as 15 deixaria FKs sem pai).
    #
    # Para ordenar só um subconjunto (ex.: as 15 do CDB), passe apenas=QUINZE:
    # QUINZE = [
    #     "CARTEIRA_COMITENTE", "CARTEIRA_PARTICIPANTE", "ESPECIFICACAO",
    #     "ESPECIFICACAO_COMITENTE", "INSTRUMENTO_FINANCEIRO", "LANCAMENTO",
    #     "OPERACAO", "TITULO", "EVENTO", "CONDICAO_IF", "CREDITO",
    #     "DADO_OPERACAO", "DEPOSITO_AUTOMATICO_IF", "JUROS_FLUTUANTE", "RESGATE",
    # ]
    # rel = ordem_insercao(SPECS, apenas=QUINZE)

    rel = ordem_insercao(SPECS)
    imprime_relatorio(rel)
