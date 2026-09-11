from contextlib import contextmanager
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from datagen import engorda_tables as engorda


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("engorda-pk-allocation-test")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@contextmanager
def spark_config(spark, **options):
    previous = {key: spark.conf.get(key) for key in options}
    try:
        for key, value in options.items():
            spark.conf.set(key, value)
        yield
    finally:
        for key, value in previous.items():
            spark.conf.set(key, value)


@pytest.mark.parametrize("adaptive", ["false", "true"])
def test_parquet_decimal_reserved_slots_survive_range_resampling(spark, tmp_path, adaptive):
    count, factor, start, step = 4096, 2, 100_000, 7
    path = str(tmp_path / "source")
    # Non-integral Decimal keys guard numeric (not string or truncated) ordering.
    spark.range(count).selectExpr(
        "cast(id * 3 + 0.125 as decimal(38,10)) as ID", "id as SOURCE_RANK"
    ).write.parquet(path)
    source = spark.read.parquet(path)
    plan = engorda.PlanoTabela(
        "T", ("ID",), pk_regra="OFFSET_PROPRIO", pk_start=start, pk_passo=step
    )
    with spark_config(
        spark,
        **{
            "spark.sql.adaptive.enabled": adaptive,
            "spark.sql.shuffle.partitions": "32",
            "spark.sql.execution.rangeExchange.sampleSizePerPartition": "1",
        },
    ):
        clones, mapping = engorda.clona_tabela(spark, plan, source, factor, {})
        try:
            expected = (
                F.lit(start)
                + (
                    ((F.col("old_ID") - F.lit(Decimal("0.125"))) / 3).cast("long") * factor
                    + F.col(engorda.K_COL)
                    - 1
                )
                * step
            )
            stats = mapping.agg(
                F.count("*").alias("rows"),
                F.countDistinct("new_ID").alias("unique"),
                F.min("new_ID").alias("minimum"),
                F.max("new_ID").alias("maximum"),
                F.sum((F.col("new_ID") != expected).cast("long")).alias("wrong_rank"),
            ).first()
            assert stats.asDict() == {
                "rows": count * factor,
                "unique": count * factor,
                "minimum": start,
                "maximum": start + (count * factor - 1) * step,
                "wrong_rank": 0,
            }
            assert clones.count() == count * factor
            assert clones.select("ID").distinct().count() == count * factor
            assert (
                clones.where(
                    (F.col("ID") < start + F.col("SOURCE_RANK") * factor * step)
                    | (F.col("ID") > start + (F.col("SOURCE_RANK") * factor + factor - 1) * step)
                ).count()
                == 0
            )
            with spark_config(spark, **{"spark.sql.shuffle.partitions": "11"}):
                _, repartitioned = engorda.clona_tabela(
                    spark, plan, source.repartition(7), factor, {}
                )
                try:
                    assert mapping.exceptAll(repartitioned).count() == 0
                    assert repartitioned.exceptAll(mapping).count() == 0
                finally:
                    repartitioned._jdf.logicalPlan().rdd().unpersist(False)
        finally:
            mapping._jdf.logicalPlan().rdd().unpersist(False)


def test_mapping_owns_freeze_and_releases_only_temporary_snapshot(spark, monkeypatch):
    source = spark.range(64).withColumnRenamed("id", "ID").localCheckpoint(eager=True)
    frame_class = type(source)
    original_checkpoint = frame_class.localCheckpoint
    original_collect = frame_class.collect
    checkpoints, plans, collected_columns = [], [], []
    eager_modes = []
    baseline = spark.sparkContext._jsc.sc().getPersistentRDDs().size()

    def checkpoint(frame, eager=True):
        eager_modes.append(eager)
        plans.append(frame._jdf.queryExecution().executedPlan().toString())
        result = original_checkpoint(frame, eager=eager)
        checkpoints.append(result)
        return result

    def collect(frame):
        collected_columns.append(frame.columns)
        return original_collect(frame)

    plan = engorda.PlanoTabela("T", ("ID",), pk_regra="OFFSET_PROPRIO", pk_start=100)
    mapping = None
    try:
        with monkeypatch.context() as patch:
            patch.setattr(frame_class, "localCheckpoint", checkpoint)
            patch.setattr(frame_class, "collect", collect)
            _, mapping = engorda.clona_tabela(spark, plan, source, 2, {})
        assert len(checkpoints) == 2  # Sorted snapshot, then final map; no third clone checkpoint.
        assert eager_modes == [False, True]
        assert checkpoints[-1] is mapping
        assert collected_columns == [["____pk_rid_part", "__sz"]]
        assert all("SinglePartition" not in plan for plan in plans)
        assert "Window" not in plans[0]
        assert "Window" in plans[1]
        assert not checkpoints[0]._jdf.logicalPlan().rdd().getStorageLevel().useMemory()
        assert mapping._jdf.logicalPlan().rdd().getStorageLevel().useMemory()
        assert spark.sparkContext._jsc.sc().getPersistentRDDs().size() == baseline + 1
        source._jdf.logicalPlan().rdd().unpersist(True)
        expected = [(i, k, 100 + i * 2 + k - 1) for i in range(64) for k in (1, 2)]
        for _ in range(2):
            assert [tuple(row) for row in mapping.orderBy("old_ID", engorda.K_COL).collect()] == (
                expected
            )
    finally:
        source._jdf.logicalPlan().rdd().unpersist(False)
        if mapping is not None:
            mapping._jdf.logicalPlan().rdd().unpersist(False)
    assert spark.sparkContext._jsc.sc().getPersistentRDDs().size() == baseline - 1


def test_temporary_snapshot_is_released_on_initial_materialization_failure(spark):
    source = spark.range(0, 4, numPartitions=2).selectExpr(
        "id",
        "CASE WHEN id = 3 THEN raise_error('injected initial_materialization failure') "
        "ELSE 'ok' END AS payload",
    )
    baseline = spark.sparkContext._jsc.sc().getPersistentRDDs().size()
    yielded = False
    with pytest.raises(Exception, match="injected initial_materialization failure"):
        with engorda._with_contiguous_row_id(source, "rid"):
            yielded = True
    assert not yielded
    assert spark.sparkContext._jsc.sc().getPersistentRDDs().size() == baseline


@pytest.mark.parametrize("failure", ["sizes", "consumer", "mapping_checkpoint"])
def test_temporary_snapshot_is_released_on_failure(spark, monkeypatch, failure):
    source = spark.range(8).withColumnRenamed("id", "ID")
    frame_class = type(source)
    original_checkpoint = frame_class.localCheckpoint
    checkpoints = []
    baseline = spark.sparkContext._jsc.sc().getPersistentRDDs().size()

    def checkpoint(frame, eager=True):
        if failure == "mapping_checkpoint" and checkpoints:
            raise RuntimeError("injected mapping_checkpoint failure")
        result = original_checkpoint(frame, eager=eager)
        checkpoints.append(result)
        return result

    def failed_collect(frame):
        raise RuntimeError("injected sizes failure")

    monkeypatch.setattr(frame_class, "localCheckpoint", checkpoint)
    if failure == "sizes":
        monkeypatch.setattr(frame_class, "collect", failed_collect)
    with pytest.raises(RuntimeError, match=f"injected {failure} failure"):
        if failure == "mapping_checkpoint":
            plan = engorda.PlanoTabela("T", ("ID",), pk_regra="OFFSET_PROPRIO", pk_start=100)
            engorda.clona_tabela(spark, plan, source, 2, {})
        else:
            with engorda._with_contiguous_row_id(source, "rid"):
                raise RuntimeError("injected consumer failure")
    assert len(checkpoints) == 1
    assert not checkpoints[0]._jdf.logicalPlan().rdd().getStorageLevel().useMemory()
    assert spark.sparkContext._jsc.sc().getPersistentRDDs().size() == baseline


@pytest.mark.parametrize("values", [[], [(None,), (2,), (None,), (1,)]])
def test_numbering_preserves_empty_and_null_payloads(spark, values):
    source = spark.createDataFrame(values, "ID long").repartition(3)
    with engorda._with_contiguous_row_id(source.orderBy("ID"), "rid") as numbered:
        rows = numbered.orderBy("rid").collect()
        assert [row.rid for row in rows] == list(range(len(values)))
        assert [row.ID for row in rows] == sorted(
            [value[0] for value in values], key=lambda value: (value is not None, value or 0)
        )
        assert numbered.columns == ["ID", "rid"]


@pytest.mark.parametrize("suffix", ["part", "prow", "poff", "mid"])
def test_numbering_rejects_temporary_column_collisions(spark, suffix):
    source = spark.range(1).withColumn(f"__rid_{suffix}", F.lit(0))
    baseline = spark.sparkContext._jsc.sc().getPersistentRDDs().size()
    with pytest.raises(ValueError, match="colis.*coluna tempor"):
        with engorda._with_contiguous_row_id(source, "rid"):
            pytest.fail("temporary collision was accepted")
    assert spark.sparkContext._jsc.sc().getPersistentRDDs().size() == baseline


def test_parent_own_pk_and_shared_child_preserve_fk_per_clone(spark, monkeypatch):
    broadcast = F.broadcast

    def bounded_broadcast(frame):
        assert not any(column.startswith(("old_", "new_")) for column in frame.columns), (
            "Complete PK maps must not be forced into driver-side broadcasts"
        )
        return broadcast(frame)

    monkeypatch.setattr(F, "broadcast", bounded_broadcast)
    root = spark.createDataFrame([(20,), (10,)], "ROOT long")
    parent = spark.createDataFrame([(3, 10), (1, 20), (2, 10)], "ID long, ROOT long")
    child = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], "ID long, SIDE string")
    root_plan = engorda.PlanoTabela(
        "ROOT", ("ROOT",), pk_regra="OFFSET_PROPRIO", pk_start=100, pk_passo=5
    )
    parent_plan = engorda.PlanoTabela(
        "PARENT",
        ("ID",),
        [engorda.FkRemap(("ROOT",), "ROOT", ("ROOT",), True)],
        pk_regra="OFFSET_PROPRIO",
        pk_start=1000,
        pk_passo=7,
    )
    child_plan = engorda.PlanoTabela(
        "CHILD",
        ("ID", "SIDE"),
        [engorda.FkRemap(("ID",), "PARENT", ("ID",), True)],
        pk_regra="VIA_PAI",
    )
    mappings = {}
    try:
        _, mappings["ROOT"] = engorda.clona_tabela(spark, root_plan, root, 3, {})
        parents, mappings["PARENT"] = engorda.clona_tabela(spark, parent_plan, parent, 3, mappings)
        children, mappings["CHILD"] = engorda.clona_tabela(spark, child_plan, child, 3, mappings)
        assert sorted(tuple(row) for row in parents.collect()) == sorted(
            (1000 + ((pk - 1) * 3 + k - 1) * 7, 100 + ((root_pk // 10 - 1) * 3 + k - 1) * 5)
            for pk, root_pk in [(1, 20), (2, 10), (3, 10)]
            for k in (1, 2, 3)
        )
        assert sorted(tuple(row) for row in children.collect()) == sorted(
            (1000 + ((pk - 1) * 3 + k - 1) * 7, side)
            for pk, side in [(1, "a"), (2, "b"), (3, "c")]
            for k in (1, 2, 3)
        )
        # VIA_PAI owns its final freeze too: it must survive loss of the parent map.
        mappings["PARENT"]._jdf.logicalPlan().rdd().unpersist(True)
        expected = sorted(
            (pk, side, k, 1000 + ((pk - 1) * 3 + k - 1) * 7, side)
            for pk, side in [(1, "a"), (2, "b"), (3, "c")]
            for k in (1, 2, 3)
        )
        for _ in range(2):
            assert sorted(tuple(row) for row in mappings["CHILD"].collect()) == expected
    finally:
        for mapping in mappings.values():
            mapping._jdf.logicalPlan().rdd().unpersist(False)
