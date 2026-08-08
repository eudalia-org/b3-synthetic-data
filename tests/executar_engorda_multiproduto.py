from engorda_instrumentos import EngordaJob, executar_job


# As regras de cada produto ficam no dicionário REGRAS_PRODUTO,
# dentro de engorda_instrumentos.py. Este runner escolhe apenas o produto e o run.
JOB = EngordaJob(
    produto="cdb_simplificado", #aplica as regras especificas de cdb_simplificado que estao no codigo
    # Use n_instrumentos ou num_ifs=(123, 456), nunca os dois.
    n_instrumentos=5,
    fator_k=3,
    dry_run=False,
)


if __name__ == "__main__":
    executar_job(JOB)
