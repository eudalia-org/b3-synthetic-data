from engorda_instrumentos import EngordaJob, executar_job


# As regras de cada produto ficam no dicionário REGRAS_PRODUTO,
# dentro de engorda_instrumentos.py. Este runner escolhe apenas o produto e o run.
JOB = EngordaJob(
    produto="cdb_simplificado",
    # Use n_instrumentos ou num_ifs=(123, 456), nunca os dois.
    n_instrumentos=5,
    fator_k=3,
    meu_numero_prefix="321",
    dry_run=True,
)


if __name__ == "__main__":
    executar_job(JOB)
