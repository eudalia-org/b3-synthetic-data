import json
s = json.load(open("spec_config.json"))
edges = [("CONTA_PARTICIPANTE","PARTICIPANTE"), ("COMITENTE","PAIS"),
         ("PESSOA_JURIDICA","PAIS"), ("CONTEXTO_MENSAGEM","TIPO_ESTADO"),
         ("LOTE","TIPO_IF"), ("PARTICIPANTE","PESSOA_JURIDICA")]
for c, p in edges:
    fks = (s.get(c) or {}).get("foreign_keys") or []
    tem = any(f["parent_table"] == p for f in fks)
    print(f"{c} -> {p}: aresta={tem} | filho_no_spec={c in s} pai_no_spec={p in s}")
for t in ["DETENTOR_IF","MOTIVO_SITUACAO_IF","UNIDADE_MEDIDA",
          "VEICULO_GARANTIDOR","CONTA_PARTICIPANTE","PARTICIPANTE"]:
    cfg = s.get(t) or {}
    print(f"{t}: not_null_cols={len(cfg.get('not_null_cols') or [])} static={cfg.get('static')}")
