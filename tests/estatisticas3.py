# =====================================================================
# COMPARAÇÃO ESTATÍSTICA — SINTÉTICO vs ORIGINAL  (big numbers + ranges)
#
# Cole esta célula no notebook DEPOIS da célula que define:
#   spark, original_path, synthetic_path, tabelas_para_validar
#
# Saída:
#   1. Linha de big numbers (tabelas, linhas, colunas, cobertura de range)
#   2. Volume de linhas por tabela (Original vs Sintético + % do original)
#   3. Uma figura por tabela: histograma de distribuição por coluna numérica,
#      Original × Sintético sobrepostos, com mín/méd/máx de cada lado.
#      Bins comuns aos dois lados, cortados em p1–p99 combinados (senão a
#      cauda pesada esmaga o histograma); % relativo às linhas da amostra.
#   4. df_resumo (por tabela) e df_estatisticas (por coluna) para consulta
#
# Tabelas gigantes: contagem de linhas sempre exata; mín/máx exatos por padrão
# (MINMAX_EXATO) para a cobertura de range não sair inflada; média/desvio/
# percentis/histograma numa amostra reprodutível (df.sample). O sample NÃO
# evita o scan das colunas — o custo por tabela é ~2 leituras das colunas
# escolhidas por lado (1 com MINMAX_EXATO=False). Rode a célula antes da
# apresentação e deixe as figuras prontas no notebook.
# =====================================================================
import math

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.ticker import FuncFormatter, MaxNLocator
from IPython.display import display
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ---------------- configuração ----------------
MAX_COLUNAS_POR_TABELA = 6        # colunas numéricas por tabela no gráfico de ranges
PREFIXOS_MEDIDA = ("VAL", "QTD", "TAX", "PCT", "PRC", "FATOR", "SALDO")  # priorizadas
PRECISAO_PERCENTIL = 10000        # accuracy do approx_percentile
NUM_BINS = 30                     # bins dos histogramas de distribuição
TAMANHO_AMOSTRA = 1_000_000       # linhas-alvo p/ média/desvio/percentis/histograma
                                  # (None = tabela inteira). O sample NÃO reduz o
                                  # scan das colunas; barateia os sketches e a
                                  # passada do histograma (amostra fica em cache)
SEED_AMOSTRA = 42                 # amostragem reprodutível entre execuções
MINMAX_EXATO = True               # mín/máx na tabela COMPLETA (1 scan extra por
                                  # lado). False = mín/máx da amostra: mais rápido,
                                  # porém subestima o range e infla a cobertura

# ---------------- paleta ----------------
COR_ORIGINAL  = "#2a78d6"   # azul       — Original
COR_SINTETICO = "#1baf7a"   # verde-água — Sintético
SUPERFICIE    = "#fcfcfb"
TINTA         = "#0b0b0b"
TINTA_2       = "#52514e"
TINTA_MUTED   = "#898781"
COR_EIXO      = "#c3c2b7"
COR_GRADE     = "#e1e0d9"

ESTILO = {
    "figure.facecolor": SUPERFICIE, "axes.facecolor": SUPERFICIE,
    "savefig.facecolor": SUPERFICIE,
    "font.family": "sans-serif",
    "font.sans-serif": ["Segoe UI", "DejaVu Sans", "Arial"],
    "text.color": TINTA, "axes.labelcolor": TINTA_2,
    "xtick.color": TINTA_MUTED, "ytick.color": TINTA_2,
    "axes.edgecolor": COR_EIXO,
}

_TIPOS_NUMERICOS = (T.ByteType, T.ShortType, T.IntegerType, T.LongType,
                    T.FloatType, T.DoubleType, T.DecimalType)


def _fmt(v, casas=1):
    """1234567 -> '1,2 mi' (abreviação pt-BR)."""
    if v is None or (isinstance(v, float) and not math.isfinite(v)):
        return "—"
    for lim, suf in ((1e9, " bi"), (1e6, " mi"), (1e3, " mil")):
        if abs(v) >= lim:
            return f"{v / lim:.{casas}f}{suf}".replace(".", ",")
    if float(v) == int(v):
        return f"{int(v):,}".replace(",", ".")
    return f"{v:,.2f}".replace(",", "\0").replace(".", ",").replace("\0", ".")


def _le_parquet(path):
    try:
        return spark.read.parquet(path)
    except Exception:
        return None


def _colunas_numericas(df):
    return [f.name for f in df.schema.fields if isinstance(f.dataType, _TIPOS_NUMERICOS)]


def _prioriza(cols):
    """Colunas de medida (VAL_, QTD_, ...) antes de códigos/chaves."""
    def chave(c):
        for i, p in enumerate(PREFIXOS_MEDIDA):
            if c.startswith(p):
                return (0, i, c)
        return (1, 0, c)
    return sorted(cols, key=chave)


def _amostra(df, n_total):
    """Amostra reprodutível com ~TAMANHO_AMOSTRA linhas; tabela inteira se couber."""
    if not TAMANHO_AMOSTRA or not n_total or n_total <= TAMANHO_AMOSTRA:
        return df, 1.0
    frac = TAMANHO_AMOSTRA / n_total
    return df.sample(fraction=frac, seed=SEED_AMOSTRA), frac


def _estatisticas(df, cols):
    """min/max/média/desvio/p5/p50/p95 de todas as colunas em UMA passada."""
    aggs = [F.count(F.lit(1)).alias("__n")]
    for i, c in enumerate(cols):
        # nanvl: NaN de double vira NULL (Spark ordena NaN acima de tudo, o
        # que contaminaria min/max/percentis; NULL é ignorado)
        d = F.nanvl(F.col(c).cast("double"), F.lit(None).cast("double"))
        aggs += [
            F.min(d).alias(f"{i}__min"),
            F.max(d).alias(f"{i}__max"),
            F.mean(d).alias(f"{i}__media"),
            F.stddev(d).alias(f"{i}__dp"),
            F.count(d).alias(f"{i}__nv"),
            F.expr(f"approx_percentile(nanvl(CAST(`{c}` AS DOUBLE), "
                   f"CAST(NULL AS DOUBLE)), "
                   f"array(0.01, 0.05, 0.5, 0.95, 0.99), "
                   f"{PRECISAO_PERCENTIL})").alias(f"{i}__pct"),
        ]
    row = df.agg(*aggs).first().asDict()
    saida = {"linhas_amostra": row["__n"], "cols": {}}
    for i, c in enumerate(cols):
        pct = row[f"{i}__pct"] or [None] * 5
        saida["cols"][c] = {
            "min": row[f"{i}__min"], "max": row[f"{i}__max"],
            "media": row[f"{i}__media"], "dp": row[f"{i}__dp"],
            "n_validos": row[f"{i}__nv"],
            "p01": pct[0], "p05": pct[1], "p50": pct[2],
            "p95": pct[3], "p99": pct[4],
        }
    return saida


def _num(v):
    """None para valor ausente ou NaN (NaN se comporta diferente de NULL)."""
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    return v


def _minmax_exato(df, cols):
    """min/max exatos na tabela completa (scan só das colunas escolhidas)."""
    if not cols:
        return {}
    aggs = []
    for i, c in enumerate(cols):
        d = F.nanvl(F.col(c).cast("double"), F.lit(None).cast("double"))
        aggs += [F.min(d).alias(f"{i}__min"), F.max(d).alias(f"{i}__max")]
    row = df.agg(*aggs).first().asDict()
    return {c: (row[f"{i}__min"], row[f"{i}__max"])
            for i, c in enumerate(cols)}


def _define_bins(o, s, inteira):
    """Bordas de bin comuns aos dois lados: p1–p99 combinados; domínio inteiro
    pequeno ganha um bin por valor."""
    los = [v for v in (o["p01"], s["p01"]) if v is not None]
    his = [v for v in (o["p99"], s["p99"]) if v is not None]
    if not los or not his:
        return None
    lo, hi = min(los), max(his)
    if hi <= lo:
        return [lo - 0.5, lo + 0.5]
    if inteira and hi - lo <= NUM_BINS * 1.5:
        return [x - 0.5 for x in range(int(math.floor(lo)), int(math.ceil(hi)) + 2)]
    return [float(v) for v in np.linspace(lo, hi, NUM_BINS + 1)]


def _histogramas(df, edges_por_col):
    """Contagens por bin de todas as colunas em UMA passada de agregação."""
    aggs = []
    for c, edges in edges_por_col.items():
        if edges is None:
            continue
        d = F.nanvl(F.col(c).cast("double"), F.lit(None).cast("double"))
        for b in range(len(edges) - 1):
            ultimo = b == len(edges) - 2
            cond = (d >= edges[b]) & ((d <= edges[b + 1]) if ultimo
                                      else (d < edges[b + 1]))
            aggs.append(F.count(F.when(cond, 1)).alias(f"{c}__{b}"))
    if not aggs:
        return {c: None for c in edges_por_col}
    row = df.agg(*aggs).first().asDict()
    return {c: (None if edges is None
                else [row[f"{c}__{b}"] for b in range(len(edges) - 1)])
            for c, edges in edges_por_col.items()}


def _cobertura(o, s):
    """% do range original [min,max] coberto pelo range sintético."""
    om, ox = _num(o["min"]), _num(o["max"])
    sm, sx = _num(s["min"]), _num(s["max"])
    if None in (om, ox, sm, sx):
        return None
    span = ox - om
    if span == 0:
        return 100.0 if sm <= om <= sx else 0.0
    inter = min(ox, sx) - max(om, sm)
    return max(0.0, min(1.0, inter / span)) * 100.0


def _limpa_eixos(ax, esconde=("top", "right")):
    for lado in ("top", "right", "left", "bottom"):
        if lado in esconde:
            ax.spines[lado].set_visible(False)
        else:
            ax.spines[lado].set_color(COR_EIXO)
            ax.spines[lado].set_linewidth(0.8)
    ax.tick_params(length=0)


# ---------------- coleta (Spark) ----------------
resultados, resumo = {}, []
houve_amostra = False
for tabela in tabelas_para_validar:
    df_o = _le_parquet(f"{original_path}/{tabela}")
    df_s = _le_parquet(f"{synthetic_path}/{tabela}")
    if df_o is None or df_s is None:
        faltando = "original" if df_o is None else "sintético"
        resumo.append({"tabela": tabela, "status": f"sem parquet {faltando}",
                       "linhas_original": None, "linhas_sintetico": None,
                       "pct_do_original": None, "colunas_comparadas": 0,
                       "cobertura_media_range_pct": None,
                       "amostra_original_pct": None})
        print(f"✗ {tabela}: sem parquet {faltando}")
        continue

    comuns = [c for c in _colunas_numericas(df_o) if c in set(_colunas_numericas(df_s))]
    escolhidas = _prioriza(comuns)[:MAX_COLUNAS_POR_TABELA]
    inteiras = {f.name for f in df_o.schema.fields
                if isinstance(f.dataType, (T.ByteType, T.ShortType,
                                           T.IntegerType, T.LongType))
                or (isinstance(f.dataType, T.DecimalType)
                    and f.dataType.scale == 0)}

    # contagem exata (barata no parquet); amostra p/ as demais estatísticas
    n_orig, n_sint = df_o.count(), df_s.count()
    am_o, frac_o = _amostra(df_o, n_orig)
    am_s, frac_s = _amostra(df_s, n_sint)
    houve_amostra = houve_amostra or frac_o < 1.0 or frac_s < 1.0
    if escolhidas:  # cache: a passada do histograma relê só a amostra
        am_o = am_o.select(*escolhidas).persist()
        am_s = am_s.select(*escolhidas).persist()
    est_o = _estatisticas(am_o, escolhidas)
    est_s = _estatisticas(am_s, escolhidas)
    est_o["linhas"], est_s["linhas"] = n_orig, n_sint
    if MINMAX_EXATO:  # mín/máx de amostra subestimam o range (infla cobertura)
        for est, df_full, frac in ((est_o, df_o, frac_o), (est_s, df_s, frac_s)):
            if frac < 1.0:
                for c, (mn, mx) in _minmax_exato(df_full, escolhidas).items():
                    est["cols"][c]["min"], est["cols"][c]["max"] = mn, mx

    # histogramas com bins comuns aos dois lados, uma passada por lado
    edges_tab = {c: _define_bins(est_o["cols"][c], est_s["cols"][c],
                                 c in inteiras) for c in escolhidas}
    hist_o = _histogramas(am_o, edges_tab)
    hist_s = _histogramas(am_s, edges_tab)
    if escolhidas:
        am_o.unpersist()
        am_s.unpersist()

    cobs = [_cobertura(est_o["cols"][c], est_s["cols"][c]) for c in escolhidas]
    cobs = [c for c in cobs if c is not None]
    resultados[tabela] = {"orig": est_o, "sint": est_s,
                          "colunas": escolhidas, "n_comuns": len(comuns),
                          "edges": edges_tab,
                          "hist_o": hist_o, "hist_s": hist_s}
    resumo.append({
        "tabela": tabela, "status": "ok",
        "linhas_original": n_orig, "linhas_sintetico": n_sint,
        "pct_do_original": round(n_sint / n_orig * 100, 2) if n_orig else None,
        "colunas_comparadas": len(escolhidas),
        "cobertura_media_range_pct": round(float(np.mean(cobs)), 1) if cobs else None,
        "amostra_original_pct": round(frac_o * 100, 2),
    })
    nota_amostra = (f" · stats em amostra de {_fmt(est_o['linhas_amostra'])}"
                    + f" ({frac_o * 100:.2f}%)".replace(".", ",")
                    if frac_o < 1.0 else "")
    print(f"✓ {tabela}: orig {_fmt(n_orig)} × sint {_fmt(n_sint)}"
          f" · {len(escolhidas)}/{len(comuns)} colunas numéricas{nota_amostra}")

ok = [t for t in tabelas_para_validar if t in resultados]
df_resumo = pd.DataFrame(resumo)
if not ok:
    display(df_resumo)
    raise RuntimeError("Nenhuma tabela pôde ser lida nos dois caminhos — "
                       "verifique original_path / synthetic_path.")

# tabela detalhada por coluna (fica disponível para consulta)
_linhas = []
for t in ok:
    for c in resultados[t]["colunas"]:
        o, s = resultados[t]["orig"]["cols"][c], resultados[t]["sint"]["cols"][c]
        _linhas.append({"tabela": t, "coluna": c,
                        "min_orig": o["min"], "min_sint": s["min"],
                        "max_orig": o["max"], "max_sint": s["max"],
                        "media_orig": o["media"], "media_sint": s["media"],
                        "dp_orig": o["dp"], "dp_sint": s["dp"],
                        "p50_orig": o["p50"], "p50_sint": s["p50"],
                        "cobertura_range_pct": _cobertura(o, s)})
df_estatisticas = pd.DataFrame(_linhas)

# ---------------- gráficos ----------------
with plt.rc_context(ESTILO):
    # ---- 1. big numbers -------------------------------------------------
    tot_o = sum(resultados[t]["orig"]["linhas"] for t in ok)
    tot_s = sum(resultados[t]["sint"]["linhas"] for t in ok)
    n_cols = int(df_resumo.loc[df_resumo["status"] == "ok", "colunas_comparadas"].sum())
    cob_geral = (df_estatisticas["cobertura_range_pct"].dropna()
                 if "cobertura_range_pct" in df_estatisticas
                 else pd.Series(dtype=float))
    kpis = [
        ("TABELAS COMPARADAS", f"{len(ok)}/{len(tabelas_para_validar)}",
         "com parquet nos dois lados"),
        ("LINHAS — ORIGINAL", _fmt(tot_o), "soma das tabelas comparadas"),
        ("LINHAS — SINTÉTICO", _fmt(tot_s),
         f"{tot_s / tot_o * 100:.1f}% do original".replace(".", ",")
         if tot_o else ""),
        ("COLUNAS NUMÉRICAS", f"{n_cols}", "comparadas nos ranges"),
        ("COBERTURA DE RANGE", f"{cob_geral.mean():.0f}%" if len(cob_geral) else "—",
         "média · range original coberto"
         + (" · em amostra" if houve_amostra and not MINMAX_EXATO else "")),
    ]
    fig, eixos = plt.subplots(1, len(kpis), figsize=(3.2 * len(kpis), 2.1))
    fig.subplots_adjust(left=0.005, right=0.995, top=0.80, bottom=0.05)
    fig.suptitle("Sintético vs Original — visão estatística", x=0.005, y=0.97,
                 ha="left", fontsize=13, fontweight="bold", color=TINTA)
    for ax, (rotulo, valor, sub) in zip(eixos, kpis):
        ax.axis("off")
        ax.text(0, 0.80, rotulo, fontsize=8.5, color=TINTA_2)
        ax.text(0, 0.28, valor, fontsize=27, fontweight="bold", color=TINTA)
        ax.text(0, 0.02, sub, fontsize=8.5, color=TINTA_MUTED)
    plt.show()

    # ---- 2. volume de linhas por tabela ---------------------------------
    ordem = sorted(ok, key=lambda t: resultados[t]["orig"]["linhas"])  # maior no topo
    n_o = np.array([resultados[t]["orig"]["linhas"] for t in ordem], dtype=float)
    n_s = np.array([resultados[t]["sint"]["linhas"] for t in ordem], dtype=float)
    razao = np.where(n_o > 0, n_s / n_o * 100.0, np.nan)
    y = np.arange(len(ordem))

    fig, (ax1, ax2) = plt.subplots(
        1, 2, figsize=(14, 0.52 * len(ordem) + 1.8), sharey=True,
        gridspec_kw={"width_ratios": [3, 1], "wspace": 0.06},
        constrained_layout=True)
    ax1.barh(y + 0.20, n_o, height=0.36, color=COR_ORIGINAL, label="Original")
    ax1.barh(y - 0.20, n_s, height=0.36, color=COR_SINTETICO, label="Sintético")
    desloc = n_o.max() * 0.012
    for yi, v in zip(y + 0.20, n_o):
        ax1.text(v + desloc, yi, _fmt(v), fontsize=8, color=TINTA_2, va="center")
    for yi, v in zip(y - 0.20, n_s):
        ax1.text(v + desloc, yi, _fmt(v), fontsize=8, color=TINTA_2, va="center")
    ax1.set_yticks(y)
    ax1.set_yticklabels(ordem, fontsize=9)
    ax1.set_xlim(0, n_o.max() * 1.20)
    ax1.xaxis.set_major_formatter(FuncFormatter(lambda v, _p: _fmt(v, 0)))
    ax1.xaxis.set_major_locator(MaxNLocator(4))
    ax1.grid(axis="x", color=COR_GRADE, linewidth=0.6)
    ax1.set_axisbelow(True)
    ax1.legend(loc="lower right", frameon=False, fontsize=9)
    ax1.set_title("Volume de linhas — Original vs Sintético",
                  loc="left", fontsize=11, fontweight="bold", color=TINTA)
    _limpa_eixos(ax1)

    ax2.barh(y, razao, height=0.5, color=COR_SINTETICO)
    ax2.axvline(100, color=COR_EIXO, linewidth=1)
    lim2 = max(120.0, np.nanmax(razao) * 1.30) if np.isfinite(razao).any() else 120.0
    for yi, v in zip(y, razao):
        if math.isfinite(v):
            ax2.text(v + lim2 * 0.02, yi, f"{v:.1f}%".replace(".", ","),
                     fontsize=8, color=TINTA_2, va="center")
    ax2.set_xlim(0, lim2)
    ax2.set_xticks([0, 100])
    ax2.set_xticklabels(["0", "100%"], fontsize=8)
    ax2.set_title("Sintético como % do original",
                  loc="left", fontsize=10, color=TINTA_2)
    _limpa_eixos(ax2)
    plt.show()

    # ---- 3. distribuições por coluna (uma figura por tabela) ------------
    tabs_dist = [t for t in ok if resultados[t]["colunas"]]
    if tabs_dist:
        nota = ("Histogramas: bins comuns aos dois lados, cortados em p1–p99 "
                "combinados; % relativo às linhas válidas de cada lado; "
                "linha vertical = média.")
        if MINMAX_EXATO:
            nota += " Mín/máx exatos (tabela completa)."
        elif houve_amostra:
            nota += " Mín/máx estimados na amostra."
        print(nota)
    for tabela in tabs_dist:
        info = resultados[tabela]
        cols = info["colunas"]
        nlin = math.ceil(len(cols) / 3)
        fig, eixos = plt.subplots(nlin, 3, figsize=(15.5, 2.8 * nlin + 0.7),
                                  squeeze=False)
        fig.suptitle(f"{tabela} — distribuições por coluna · "
                     f"orig {_fmt(info['orig']['linhas'])} × "
                     f"sint {_fmt(info['sint']['linhas'])} linhas",
                     x=0.005, y=0.99, ha="left", va="top",
                     fontsize=12, fontweight="bold", color=TINTA)
        fig.legend(handles=[Line2D([], [], color=COR_ORIGINAL, lw=5,
                                   label="Original"),
                            Line2D([], [], color=COR_SINTETICO, lw=5,
                                   label="Sintético")],
                   loc="upper right", bbox_to_anchor=(0.995, 0.998),
                   frameon=False, fontsize=9, ncol=2)

        for ax, c in zip(eixos.flat, cols):
            ax.set_title(c, loc="left", fontsize=9, fontweight="bold",
                         color=TINTA)
            edges = info["edges"][c]
            if edges is None:
                ax.text(0.5, 0.5, "sem valores numéricos", fontsize=8,
                        color=TINTA_MUTED, ha="center", va="center",
                        transform=ax.transAxes)
                ax.set_xticks([])
                ax.set_yticks([])
                _limpa_eixos(ax, esconde=("top", "right", "left", "bottom"))
                continue
            e = np.asarray(edges, dtype=float)
            pico = 0.0
            for lado, cor in (("orig", COR_ORIGINAL), ("sint", COR_SINTETICO)):
                est = info[lado]["cols"][c]
                cont = (info["hist_o"] if lado == "orig" else info["hist_s"])[c]
                nv = est["n_validos"]
                if not nv or cont is None:
                    continue
                pct = np.asarray(cont, dtype=float) / nv * 100.0
                pico = max(pico, float(pct.max()))
                ax.stairs(pct, e, fill=True, color=cor, alpha=0.28)
                ax.stairs(pct, e, color=cor, linewidth=1.6)
                media = _num(est["media"])
                if media is not None and e[0] <= media <= e[-1]:
                    ax.axvline(media, color=cor, linewidth=1.1, ymax=0.60)
            # mín/méd/máx por lado, no topo do subplot
            for lado, cor, ytx in (("orig", COR_ORIGINAL, 0.985),
                                   ("sint", COR_SINTETICO, 0.860)):
                est = info[lado]["cols"][c]
                if _num(est["min"]) is None:
                    txt = f"{lado}: sem valores"
                else:
                    txt = (f"{lado} · mín {_fmt(est['min'])}"
                           f" · méd {_fmt(est['media'])}"
                           f" · máx {_fmt(est['max'])}")
                ax.text(0.015, ytx, "▪", transform=ax.transAxes, color=cor,
                        fontsize=7, va="top")
                ax.text(0.055, ytx, txt, transform=ax.transAxes,
                        color=TINTA_2, fontsize=6.8, va="top")
            ax.set_xlim(e[0], e[-1])
            ax.set_ylim(0, max(pico, 1e-9) * 1.55)
            ax.yaxis.set_major_formatter(
                FuncFormatter(lambda v, _p: f"{v:.0f}%"))
            ax.yaxis.set_major_locator(MaxNLocator(3))
            # sem ticks na zona reservada ao texto nem acima de 100%
            ax.set_yticks([t for t in ax.get_yticks()
                           if t <= min(100.0, pico * 1.12)])
            ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _p: _fmt(v)))
            ax.xaxis.set_major_locator(MaxNLocator(4))
            ax.tick_params(labelsize=7)
            ax.grid(axis="y", color=COR_GRADE, linewidth=0.6)
            ax.set_axisbelow(True)
            _limpa_eixos(ax)

        for ax in eixos.flat[len(cols):]:
            ax.axis("off")
        plt.tight_layout(rect=[0, 0, 1, 1 - 0.5 / fig.get_figheight()])
        plt.show()

# ---------------- tabela-resumo (vista acessível dos mesmos números) -----
display(df_resumo)
# df_estatisticas -> detalhe por coluna (min/max/média/dp/p50 orig × sint)
