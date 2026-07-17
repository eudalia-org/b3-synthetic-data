# =====================================================================
# COMPARAÇÃO ESTATÍSTICA — SINTÉTICO vs ORIGINAL  (big numbers + ranges)
#
# Cole esta célula no notebook DEPOIS da célula que define:
#   spark, original_path, synthetic_path, tabelas_para_validar
#
# Saída:
#   1. Linha de big numbers (tabelas, linhas, colunas, cobertura de range)
#   2. Volume de linhas por tabela (Original vs Sintético + % do original)
#   3. Ranges por coluna numérica (mín–máx, p5–p95, mediana), normalizados
#      por coluna, com % de cobertura do range original pelo sintético
#   4. df_resumo (por tabela) e df_estatisticas (por coluna) para consulta
#
# Tabelas gigantes: a contagem de linhas é sempre exata, mas as estatísticas
# de range rodam sobre amostra reprodutível de ~TAMANHO_AMOSTRA linhas por
# tabela (df.sample) — ajuste a constante abaixo conforme o tempo disponível.
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
TAMANHO_AMOSTRA = 1_000_000       # linhas-alvo p/ estatísticas de range por tabela
                                  # (None = tabela inteira; contagem de linhas é
                                  # sempre exata, só as estatísticas usam amostra)
SEED_AMOSTRA = 42                 # amostragem reprodutível entre execuções

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
        d = F.col(c).cast("double")
        aggs += [
            F.min(d).alias(f"{i}__min"),
            F.max(d).alias(f"{i}__max"),
            F.mean(d).alias(f"{i}__media"),
            F.stddev(d).alias(f"{i}__dp"),
            F.expr(f"approx_percentile(CAST(`{c}` AS DOUBLE), "
                   f"array(0.05, 0.5, 0.95), {PRECISAO_PERCENTIL})").alias(f"{i}__pct"),
        ]
    row = df.agg(*aggs).first().asDict()
    saida = {"linhas_amostra": row["__n"], "cols": {}}
    for i, c in enumerate(cols):
        pct = row[f"{i}__pct"] or [None, None, None]
        saida["cols"][c] = {
            "min": row[f"{i}__min"], "max": row[f"{i}__max"],
            "media": row[f"{i}__media"], "dp": row[f"{i}__dp"],
            "p05": pct[0], "p50": pct[1], "p95": pct[2],
        }
    return saida


def _cobertura(o, s):
    """% do range original [min,max] coberto pelo range sintético."""
    if None in (o["min"], o["max"], s["min"], s["max"]):
        return None
    span = o["max"] - o["min"]
    if span == 0:
        return 100.0 if s["min"] <= o["min"] <= s["max"] else 0.0
    inter = min(o["max"], s["max"]) - max(o["min"], s["min"])
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

    # contagem exata (barata no parquet); amostra só para as estatísticas
    n_orig, n_sint = df_o.count(), df_s.count()
    am_o, frac_o = _amostra(df_o, n_orig)
    am_s, frac_s = _amostra(df_s, n_sint)
    houve_amostra = houve_amostra or frac_o < 1.0 or frac_s < 1.0
    est_o = _estatisticas(am_o, escolhidas)
    est_s = _estatisticas(am_s, escolhidas)
    est_o["linhas"], est_s["linhas"] = n_orig, n_sint

    cobs = [_cobertura(est_o["cols"][c], est_s["cols"][c]) for c in escolhidas]
    cobs = [c for c in cobs if c is not None]
    resultados[tabela] = {"orig": est_o, "sint": est_s,
                          "colunas": escolhidas, "n_comuns": len(comuns)}
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
         + (" · em amostra" if houve_amostra else "")),
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

    # ---- 3. ranges por coluna numérica ----------------------------------
    tabs_rng = [t for t in ok if resultados[t]["colunas"]]
    if tabs_rng:
        NCOLS_GRID = 3
        nlin = math.ceil(len(tabs_rng) / NCOLS_GRID)
        fig, eixos = plt.subplots(nlin, NCOLS_GRID,
                                  figsize=(15.5, 2.9 * nlin), squeeze=False)
        fig.suptitle("Ranges por coluna numérica — escala normalizada por coluna",
                     x=0.005, y=0.99, ha="left", va="top",
                     fontsize=13, fontweight="bold", color=TINTA)
        nota_rng = ("linha fina = mín–máx · barra grossa = p5–p95 · ponto = mediana"
                    " · % à direita = cobertura do range original pelo sintético")
        if houve_amostra:
            nota_rng += (f" · estatísticas estimadas em amostra de até "
                         f"{_fmt(TAMANHO_AMOSTRA, 0)} linhas/tabela")
        fig.text(0.005, 0.99 - 0.22 / fig.get_figheight(), nota_rng,
                 fontsize=8.5, color=TINTA_MUTED, va="top")
        fig.legend(handles=[Line2D([], [], color=COR_ORIGINAL, lw=5, label="Original"),
                            Line2D([], [], color=COR_SINTETICO, lw=5, label="Sintético")],
                   loc="upper right", bbox_to_anchor=(0.995, 0.998),
                   frameon=False, fontsize=9, ncol=2)

        for ax, tabela in zip(eixos.flat, tabs_rng):
            cols = resultados[tabela]["colunas"]
            for ypos, c in enumerate(reversed(cols)):
                o = resultados[tabela]["orig"]["cols"][c]
                s = resultados[tabela]["sint"]["cols"][c]
                vals = [v for v in (o["min"], o["max"], s["min"], s["max"])
                        if v is not None]
                if not vals:
                    ax.text(0.5, ypos, "sem dados", fontsize=7.5,
                            color=TINTA_MUTED, va="center", ha="center")
                    continue
                lo, span = min(vals), max(vals) - min(vals)

                def _nrm(v, lo=lo, span=span):
                    return 0.5 if span == 0 else (v - lo) / span

                for est, cor, dy, nome in ((o, COR_ORIGINAL, +0.18, "original"),
                                           (s, COR_SINTETICO, -0.18, "sintético")):
                    if est["min"] is None and est["max"] is None:
                        ax.text(0.0, ypos + dy, f"{nome}: sem valores",
                                fontsize=6.5, color=TINTA_MUTED, va="center")
                        continue
                    if est["min"] is not None and est["max"] is not None:
                        ax.plot([_nrm(est["min"]), _nrm(est["max"])],
                                [ypos + dy] * 2, color=cor, linewidth=1.4,
                                alpha=0.55, solid_capstyle="round")
                    if est["p05"] is not None and est["p95"] is not None:
                        ax.plot([_nrm(est["p05"]), _nrm(est["p95"])],
                                [ypos + dy] * 2, color=cor, linewidth=5,
                                solid_capstyle="round")
                    if est["p50"] is not None:
                        ax.plot(_nrm(est["p50"]), ypos + dy, "o", markersize=5.5,
                                color=cor, markeredgecolor=SUPERFICIE,
                                markeredgewidth=1.2)
                cob = _cobertura(o, s)
                ax.text(1.03, ypos,
                        f"{cob:.0f}%" if cob is not None else "—",
                        fontsize=7.5, color=TINTA_2, va="center")

            ax.set_title(tabela, loc="left", fontsize=10,
                         fontweight="bold", color=TINTA)
            ax.set_yticks(range(len(cols)))
            ax.set_yticklabels([c if len(c) <= 22 else c[:20] + "…"
                                for c in reversed(cols)], fontsize=8)
            ax.set_xticks([])
            ax.set_xlim(-0.03, 1.18)
            ax.set_ylim(-0.65, len(cols) - 0.35)
            _limpa_eixos(ax, esconde=("top", "right", "left", "bottom"))

        for ax in eixos.flat[len(tabs_rng):]:
            ax.axis("off")
        plt.tight_layout(rect=[0, 0, 1, 1 - 0.55 / fig.get_figheight()])
        plt.show()

# ---------------- tabela-resumo (vista acessível dos mesmos números) -----
display(df_resumo)
# df_estatisticas -> detalhe por coluna (min/max/média/dp/p50 orig × sint)
