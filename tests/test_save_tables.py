import pytest

import save_tables

# Realistic 18-character Oracle ROWIDs (base64 alphabet).
ROWID_A = "AAAS5MAAEAAAACXAAA"
ROWID_B = "AAAS5MAAEAAAACX9zz"
ROWID_C = "AAAS5MAAFAAAB2BAAA"
ROWID_D = "AAAS5MAAFAAAB2B9zz"


class TestValidateIdentifier:
    def test_uppercases_valid_identifier(self):
        assert save_tables.validate_identifier("orders") == "ORDERS"

    def test_accepts_oracle_special_characters(self):
        assert save_tables.validate_identifier("TAB_1$#") == "TAB_1$#"

    def test_rejects_injection_attempt(self):
        with pytest.raises(ValueError):
            save_tables.validate_identifier("T; DROP TABLE X")

    def test_rejects_quoted_identifier(self):
        with pytest.raises(ValueError):
            save_tables.validate_identifier('"MixedCase"')


class TestBuildRowidPredicates:
    def test_formats_between_predicates(self):
        predicates = save_tables.build_rowid_predicates(
            [(ROWID_A, ROWID_B), (ROWID_C, ROWID_D)]
        )
        assert predicates == [
            f"ROWID BETWEEN '{ROWID_A}' AND '{ROWID_B}'",
            f"ROWID BETWEEN '{ROWID_C}' AND '{ROWID_D}'",
        ]

    def test_rejects_malformed_rowid(self):
        with pytest.raises(ValueError):
            save_tables.build_rowid_predicates([("not-a-rowid", ROWID_B)])

    def test_empty_chunks_give_empty_predicates(self):
        assert save_tables.build_rowid_predicates([]) == []


def extent(index: int, blocks: int) -> tuple[str, str, int]:
    # Synthetic but pattern-valid 18-char rowids; index keeps them ordered/unique.
    start = f"AAAS5MAAEAAA{index:04d}AA"
    end = f"AAAS5MAAEAAA{index:04d}zz"
    return (start, end, blocks)


class TestMergeExtentsIntoChunks:
    def test_merges_small_extents_to_target_chunk_count(self):
        extents = [extent(i, 10) for i in range(8)]  # 80 blocks total
        chunks = save_tables.merge_extents_into_chunks(extents, num_chunks=4)
        assert len(chunks) == 4
        # Coverage: first chunk starts at first extent, last chunk ends at last extent.
        assert chunks[0][0] == extents[0][0]
        assert chunks[-1][1] == extents[-1][1]

    def test_chunk_boundaries_follow_extent_order(self):
        extents = [extent(i, 10) for i in range(6)]
        chunks = save_tables.merge_extents_into_chunks(extents, num_chunks=3)
        # Each chunk's start must be some extent's start and end some extent's end,
        # and chunks must appear in input order with no overlap or gap.
        starts = [e[0] for e in extents]
        ends = [e[1] for e in extents]
        covered = []
        for chunk_start, chunk_end in chunks:
            covered.append((starts.index(chunk_start), ends.index(chunk_end)))
        flattened = [i for pair in covered for i in range(pair[0], pair[1] + 1)]
        assert flattened == list(range(len(extents)))

    def test_fewer_extents_than_chunks(self):
        extents = [extent(0, 100), extent(1, 100)]
        chunks = save_tables.merge_extents_into_chunks(extents, num_chunks=32)
        assert len(chunks) == 2

    def test_single_extent(self):
        extents = [extent(0, 5000)]
        chunks = save_tables.merge_extents_into_chunks(extents, num_chunks=32)
        assert chunks == [(extents[0][0], extents[0][1])]

    def test_empty_extents(self):
        assert save_tables.merge_extents_into_chunks([], num_chunks=32) == []

    def test_invalid_chunk_count(self):
        assert save_tables.merge_extents_into_chunks([extent(0, 10)], num_chunks=0) == []


class TestBuildConnectionProperties:
    CONFIG = {
        "DATAGEN_SOURCE_JDBC_URL": "jdbc:oracle:thin:@host",
        "DATAGEN_SOURCE_DB_USER": "ADMIN",
        "DATAGEN_SOURCE_DB_PASSWORD": "secret",
        "DATAGEN_JDBC_FETCH_SIZE": "5000",
        "DATAGEN_JDBC_READ_TIMEOUT_MS": "600000",
        "DATAGEN_JDBC_LOB_PREFETCH": "262144",
    }

    def test_sets_read_timeout_to_break_dead_connections(self):
        properties = save_tables.build_connection_properties(self.CONFIG)
        assert properties["oracle.jdbc.ReadTimeout"] == "600000"

    def test_row_and_lob_prefetch_follow_config(self):
        properties = save_tables.build_connection_properties(self.CONFIG)
        assert properties["defaultRowPrefetch"] == "5000"
        assert properties["oracle.jdbc.defaultLobPrefetchSize"] == "262144"

    def test_core_jdbc_properties(self):
        properties = save_tables.build_connection_properties(self.CONFIG)
        assert properties["url"] == "jdbc:oracle:thin:@host"
        assert properties["user"] == "ADMIN"
        assert properties["password"] == "secret"
        assert properties["driver"] == "oracle.jdbc.OracleDriver"
        assert properties["oracle.jdbc.useFetchSizeWithLongColumn"] == "true"


class TestFetchRowidPredicates:
    def test_builds_predicates_from_extents(self, monkeypatch):
        monkeypatch.setattr(save_tables, "get_data_object_id", lambda *a: 12345)
        monkeypatch.setattr(
            save_tables,
            "fetch_extents",
            lambda *a: [(ROWID_A, ROWID_B, 64), (ROWID_C, ROWID_D, 64)],
        )
        predicates = save_tables.fetch_rowid_predicates(
            None, {}, "admin", "orders", num_partitions=2
        )
        assert predicates == [
            f"ROWID BETWEEN '{ROWID_A}' AND '{ROWID_B}'",
            f"ROWID BETWEEN '{ROWID_C}' AND '{ROWID_D}'",
        ]

    def test_returns_empty_when_object_id_missing(self, monkeypatch):
        monkeypatch.setattr(save_tables, "get_data_object_id", lambda *a: None)
        predicates = save_tables.fetch_rowid_predicates(
            None, {}, "ADMIN", "ORDERS", num_partitions=4
        )
        assert predicates == []

    def test_returns_empty_when_no_extents(self, monkeypatch):
        monkeypatch.setattr(save_tables, "get_data_object_id", lambda *a: 12345)
        monkeypatch.setattr(save_tables, "fetch_extents", lambda *a: [])
        predicates = save_tables.fetch_rowid_predicates(
            None, {}, "ADMIN", "ORDERS", num_partitions=4
        )
        assert predicates == []

    def test_rejects_bad_identifier(self):
        with pytest.raises(ValueError):
            save_tables.fetch_rowid_predicates(
                None, {}, "ADMIN", "ORDERS; DROP", num_partitions=4
            )
