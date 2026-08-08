from engorda_instrumentos_multiproduto import EngordaJob, executar_job


# As regras e o SQL de cada produto ficam em REGRAS_PRODUTO, dentro de
# engorda_instrumentos_multiproduto.py. Escolha um produto abaixo e execute
# este arquivo. Cada exemplo usa o SQL correspondente automaticamente:
#
#   cdb_simplificado -> cdb_simplificado.sql
#   cdb              -> cdb.sql (CDB completo)
#   rdb              -> rdb.sql (RDB completo)
#
# clone_prefix define a pasta abaixo de DATAGEN_SYNTHETIC_BASE_URI. Os nomes
# distintos impedem que a execução de um produto sobrescreva a de outro.
JOBS_EXEMPLO = {
    "cdb_simplificado": EngordaJob(
        produto="cdb_simplificado",
        n_instrumentos=500000,
        fator_k=2,
        clone_prefix="clones_instrumentos/cdb_simplificado",
        dry_run=False,
    ),
    "cdb": EngordaJob(
        produto="cdb",
        n_instrumentos=500000,
        fator_k=2,
        clone_prefix="clones_instrumentos/cdb_completo"
    ),
    "rdb": EngordaJob(
        produto="rdb",
        n_instrumentos=500000,
        fator_k=2,
        clone_prefix="clones_instrumentos/rdb_completo",
    ),
}

# Os prefixos 321/322/323 são exemplos; use os valores reservados no ambiente.
# Altere somente este valor para executar outro exemplo.
PRODUTO_A_EXECUTAR = "cdb_simplificado"
JOB = JOBS_EXEMPLO[PRODUTO_A_EXECUTAR]

# Para escolher NUM_IFs específicos, substitua n_instrumentos no exemplo por:
# num_ifs=(123, 456),
#
# Exemplo de execução com Spark (PowerShell, em uma única linha):
# spark-submit --py-files engorda_instrumentos_multiproduto.py --files cdb_simplificado.sql,cdb.sql,rdb.sql executar_engorda_multiproduto.py


if __name__ == "__main__":
    executar_job(JOB)
