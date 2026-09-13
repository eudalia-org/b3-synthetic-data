import dataclasses
import re
from datetime import date
from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession

from datagen import engorda_tables as E
from scripts import run_pipeline as R


@pytest.mark.parametrize(
    "month,prefix", [(1, "CDB126"), (6, "CDB626"), (10, "CDBA26"), (12, "CDBC26")]
)
def test_prefix_uses_package_month_and_operational_year(month, prefix):
    assert E._cdb_code_prefix(date(2026, 6, 3), month) == prefix


@pytest.mark.parametrize("raw_month", ["09", None, "ORA-06502"])
def test_planner_reads_package_clock_without_allocating_codes(monkeypatch, raw_month):
    connection = MagicMock()
    statement = connection.prepareStatement.return_value
    result = statement.executeQuery.return_value
    result.next.return_value = True
    result.getString.return_value = raw_month
    monkeypatch.setattr(E, "_open_oracle_connection", lambda *_: connection)
    if raw_month == "09":
        assert E._read_cdb_code_month(object(), ("url", "user", "password")) == 9
    else:
        with pytest.raises(ValueError, match="GET_DATAHOJE inválido"):
            E._read_cdb_code_month(object(), ("url", "user", "password"))
    connection.prepareStatement.assert_called_once_with(
        "SELECT TO_CHAR(CETIP.GET_DATAHOJE, 'MM') FROM dual"
    )
    connection.close.assert_called_once()


@pytest.mark.parametrize("maximum", [None, "CDB62600CNT", "CDB626B0000"])
def test_plan_starts_above_live_code_and_logged_suffixes(maximum):
    descriptor = E._synthetic_cdb_descriptor(date(2026, 6, 3), 3, maximum, code_month=6)
    assert descriptor["minimum_start"] == max(
        int("A0000", 36), int(maximum[6:], 36) + 1 if maximum else 0
    )
    assert descriptor["minimum_start"] > int("8TVJJ", 36)
    assert descriptor["prefix"] == "CDB626"
    assert descriptor["oracle_type"] == 49


def test_plan_rejects_exhaustion_instead_of_wrapping_suffix():
    with pytest.raises(ValueError, match="capacidade"):
        E._synthetic_cdb_descriptor(date(2026, 6, 3), 2, "CDB626ZZZZY", code_month=6)


def test_official_allocator_reports_captured_oracle_error_without_retry(monkeypatch):
    error_text = (
        "ORA-06502: PL/SQL: erro: buffer de string de caracteres pequeno demais "
        "numérico ou de valor"
    )
    connection = MagicMock()
    result = connection.prepareStatement.return_value.executeQuery.return_value
    result.next.side_effect = [True, False]
    result.getInt.return_value = 1
    result.getString.return_value = error_text
    opened = MagicMock(return_value=connection)
    monkeypatch.setattr(E, "_open_oracle_connection", opened)
    policy = dataclasses.replace(
        E.get_product_profile("cdb_simplificado").business_keys, cod_if_oracle_type=49
    )
    with pytest.raises(ValueError) as failure:
        list(
            E._iter_oracle_code_batches(
                object(),
                "url",
                "user",
                "password",
                code_kind="COD_IF",
                total=1,
                batch_size=1,
                engorda_date=date(2026, 6, 3),
                policy=policy,
            )
        )
    assert error_text in str(failure.value)
    assert "tipo=49 data=2026-06-03" in str(failure.value)
    opened.assert_called_once()
    connection.close.assert_called_once()


@pytest.mark.parametrize(
    "mutation",
    [
        {"prefix": "CDB726"},
        {"count": True},
        {"start": True},
        {"end": 36**5},
        {"start": 1, "end": 3},
        {"count": 4},
    ],
)
def test_generator_rejects_wrong_or_exhausted_reservation(mutation):
    start = int("A0000", 36)
    reservation = {
        "strategy": "synthetic_cdb",
        "prefix": "CDB626",
        "count": 3,
        "start": start,
        "end": start + 2,
    } | mutation
    with pytest.raises(ValueError):
        E._validate_cdb_code_range(reservation, prefix="CDB626", count=3)


def test_oracle_floor_is_read_only_and_includes_deleted_codes(monkeypatch):
    connection = MagicMock()
    statement = connection.prepareStatement.return_value
    result = statement.executeQuery.return_value
    result.next.return_value = True
    result.getString.return_value = "CDB626B0000"
    monkeypatch.setattr(E, "_open_oracle_connection", lambda *_: connection)

    assert E._read_cdb_max_code(object(), ("url", "user", "password"), "CDB626") == "CDB626B0000"
    sql = connection.prepareStatement.call_args.args[0]
    assert "NLSSORT" in sql and "NLS_SORT=BINARY" in sql
    assert "DAT_EXCLUSAO" not in sql
    assert "PKG_CODIGO" not in sql
    statement.setString.assert_any_call(1, "CDB626%")
    result.close.assert_called_once()
    statement.close.assert_called_once()
    connection.close.assert_called_once()


@pytest.mark.parametrize("collision", [False, True])
def test_live_range_preflight_checks_exact_reserved_interval(monkeypatch, collision):
    connection = MagicMock()
    statement = connection.prepareStatement.return_value
    result = statement.executeQuery.return_value
    result.next.return_value = collision
    result.getString.return_value = "CDB626A0001"
    monkeypatch.setattr(E, "_open_oracle_connection", lambda *_: connection)
    section = {"prefix": "CDB626", "count": 3, "start": int("A0000", 36), "end": int("A0002", 36)}
    if collision:
        with pytest.raises(ValueError, match="colisão COD_IF.*CDB626A0001"):
            E._assert_cdb_range_available(object(), ("url", "user", "password"), section)
    else:
        E._assert_cdb_range_available(object(), ("url", "user", "password"), section)
    statement.setString.assert_any_call(2, "CDB626A0000")
    statement.setString.assert_any_call(3, "CDB626A0002")
    assert "DAT_EXCLUSAO" not in connection.prepareStatement.call_args.args[0]
    connection.close.assert_called_once()


def test_reservations_share_prefix_across_cdb_products_and_burn_failed_ranges():
    ledger = R._new_ledger("qab")
    request = {
        "schema_version": 3,
        "plan_id": "a",
        "tables": {},
        "cod_if": E._synthetic_cdb_descriptor(date(2026, 6, 3), 3, None, code_month=6),
        "cod_operacao": {"count": 0},
        "meu_numero": {
            "ordinal_count_demand": 0,
            "operational_date": "2026-06-03",
        },
    }
    first = R._allocate_artifact(ledger, request, "a", "cdb_simplificado", "uri-a")
    # The ledger contains the first reservation even if its publication fails.
    second = R._allocate_artifact(ledger, request, "b", "cdb_resgate", "uri-b")
    assert first["cod_if"]["end"] + 1 == second["cod_if"]["start"]
    assert first["cod_if"]["start"] == int("A0000", 36)
    R._validate_ledger(ledger, "qab")


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("synthetic-cdb-codes-test")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.mark.parametrize("product", ["cdb_simplificado", "cdb_resgate"])
def test_reserved_codes_survive_repartition_without_calling_broken_allocator(
    spark, tmp_path, monkeypatch, product
):
    def broken_allocator(*_args, **_kwargs):
        pytest.fail("synthetic CDB must not call the failing Oracle allocator")

    monkeypatch.setattr(E, "_iter_oracle_code_batches", broken_allocator)
    preflight = MagicMock()
    monkeypatch.setattr(E, "_assert_cdb_range_available", preflight)
    policy = dataclasses.replace(
        E.get_product_profile(product).business_keys,
        cod_if_allocator="synthetic_cdb",
        cod_if_oracle_type=49,
    )
    start = int("A006Z", 36)
    reservation = {
        "strategy": "synthetic_cdb",
        "prefix": "CDB626",
        "count": 3,
        "start": start,
        "end": start + 2,
    }
    source = spark.createDataFrame(
        [(102, "OLD2"), (100, "OLD0"), (101, "OLD1")], "NUM_IF long, COD_IF string"
    )
    results = []
    for partitions in (1, 3):
        slots = E._code_slots(
            source.repartition(partitions), "NUM_IF", "COD_IF", "NUM_IF_NOVO", "COD_IF_ORIG"
        )
        mapping = E._materialize_code_map(
            spark,
            slots,
            code_kind="COD_IF",
            generated_alias="COD_IF_GERADO",
            out_path=str(tmp_path / str(partitions)),
            dry_run=False,
            credentials=("url", "user", "password"),
            batch_size=2,
            engorda_date=date(2026, 5, 3),  # GET_DATAHOJE was June when the plan froze its prefix.
            policy=policy,
            synthetic_reservation=reservation,
        )
        results.append({r.NUM_IF_NOVO: r.COD_IF_GERADO for r in mapping.collect()})
        instruments = E._attach_generated_code(
            source,
            mapping,
            pk_col="NUM_IF",
            new_pk_alias="NUM_IF_NOVO",
            code_col="COD_IF",
            generated_alias="COD_IF_GERADO",
        )
        operations = spark.createDataFrame(
            [(100, "STALE"), (100, "STALE"), (102, "STALE")], "NUM_IF long, COD_IF string"
        )
        propagated = E._propagate_root_cod_if(instruments, operations)
        assert sorted(r.COD_IF for r in propagated.collect()) == [
            "CDB626A006Z",
            "CDB626A006Z",
            "CDB626A0071",
        ]
    assert results == [{100: "CDB626A006Z", 101: "CDB626A0070", 102: "CDB626A0071"}] * 2
    assert all(re.fullmatch(r"CDB[1-9A-C][0-9]{2}[0-9A-Z]{5}", c) for c in results[0].values())
    assert preflight.call_count == 2


def test_synthetic_mode_rejects_other_products():
    with pytest.raises(ValueError, match="CDB"):
        E._validate_engorda_job(
            E.EngordaJob(
                produto="rdb_inclusao",
                n_instrumentos=1,
                phase="plan",
                plan_uri="plan",
                cod_if_allocator="synthetic_cdb",
            )
        )


def test_cli_and_runner_preserve_explicit_allocator(monkeypatch):
    paths = {"selection_plan": "plan", "reservations": "reservation", "synthetic": "output"}
    options = {"n_instrumentos": 2, "cod_if_allocator": "synthetic_cdb"}
    captured = []
    monkeypatch.setattr(E, "executar_job", captured.append)
    for builder in (R.build_engorda_plan_argv, R.build_engorda_materialize_argv):
        argv = builder("cdb_simplificado", "raw", "faltantes", paths, options)
        E.main(argv)
    assert [job.cod_if_allocator for job in captured] == ["synthetic_cdb"] * 2
    assert (
        R.parse_stage_overrides(["cdb_simplificado.engorda.cod_if_allocator=synthetic_cdb"])[
            "cdb_simplificado"
        ]["engorda"]["cod_if_allocator"]
        == "synthetic_cdb"
    )


def test_synthetic_mode_rejects_unreserved_execution():
    with pytest.raises(ValueError, match="plan.*materialize"):
        E._validate_engorda_job(
            E.EngordaJob(
                produto="cdb_simplificado",
                n_instrumentos=1,
                cod_if_allocator="synthetic_cdb",
            )
        )
