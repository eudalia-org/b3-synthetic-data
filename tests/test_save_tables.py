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


OBJ = 77388  # data_object_id of the AAAS5M... fixture rowid


class TestEncodeRowid:
    def test_matches_known_oracle_rowid(self):
        # AAAS5MAAEAAAACXAAA = object 77388, relative file 4, block 151, row 0.
        assert save_tables.encode_rowid(OBJ, 4, 151, 0) == "AAAS5MAAEAAAACXAAA"

    def test_max_row_number(self):
        rowid = save_tables.encode_rowid(OBJ, 4, 151, save_tables.ROWID_MAX_ROW)
        assert rowid.startswith("AAAS5MAAEAAAACX")
        assert save_tables.ROWID_PATTERN.match(rowid)

    def test_rejects_component_overflow(self):
        with pytest.raises(ValueError):
            save_tables.encode_rowid(2**40, 4, 151, 0)


class TestChunkExtents:
    def test_splits_single_large_extent(self):
        # One 100-block extent, 4 chunks of 25 blocks each.
        chunks = save_tables.chunk_extents([(4, 1000, 100)], num_chunks=4, data_object_id=OBJ)
        assert len(chunks) == 4
        assert chunks[0] == (
            save_tables.encode_rowid(OBJ, 4, 1000, 0),
            save_tables.encode_rowid(OBJ, 4, 1024, save_tables.ROWID_MAX_ROW),
        )
        assert chunks[-1] == (
            save_tables.encode_rowid(OBJ, 4, 1075, 0),
            save_tables.encode_rowid(OBJ, 4, 1099, save_tables.ROWID_MAX_ROW),
        )

    def test_merges_small_extents(self):
        extents = [(4, i * 10, 10) for i in range(8)]  # 80 blocks total
        chunks = save_tables.chunk_extents(extents, num_chunks=4, data_object_id=OBJ)
        assert len(chunks) == 4
        assert chunks[0][0] == save_tables.encode_rowid(OBJ, 4, 0, 0)
        assert chunks[-1][1] == save_tables.encode_rowid(OBJ, 4, 79, save_tables.ROWID_MAX_ROW)

    def test_chunk_can_span_files(self):
        extents = [(4, 100, 10), (5, 200, 10)]
        chunks = save_tables.chunk_extents(extents, num_chunks=1, data_object_id=OBJ)
        assert chunks == [
            (
                save_tables.encode_rowid(OBJ, 4, 100, 0),
                save_tables.encode_rowid(OBJ, 5, 209, save_tables.ROWID_MAX_ROW),
            )
        ]

    def test_full_block_coverage_without_gaps(self):
        def decode(rowid):
            alphabet = save_tables.ROWID_ALPHABET
            value = lambda chars: sum(  # noqa: E731
                alphabet.index(c) * 64**i for i, c in enumerate(reversed(chars))
            )
            return value(rowid[6:9]), value(rowid[9:15])  # (file, block)

        extents = [(4, 0, 33), (4, 64, 17), (5, 0, 50)]  # 100 blocks total
        chunks = save_tables.chunk_extents(extents, num_chunks=7, data_object_id=OBJ)
        assert 6 <= len(chunks) <= 8
        # Every extent block must fall inside exactly one chunk's [start, end] span.
        spans = [(decode(start), decode(end)) for start, end in chunks]
        for fno, block_id, blocks in extents:
            for block in range(block_id, block_id + blocks):
                hits = [s for s in spans if s[0] <= (fno, block) <= s[1]]
                assert len(hits) == 1, f"block ({fno},{block}) covered by {len(hits)} chunks"

    def test_empty_extents(self):
        assert save_tables.chunk_extents([], num_chunks=32, data_object_id=OBJ) == []

    def test_invalid_chunk_count(self):
        assert save_tables.chunk_extents([(4, 0, 10)], num_chunks=0, data_object_id=OBJ) == []


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
        monkeypatch.setattr(save_tables, "get_data_object_id", lambda *a: OBJ)
        monkeypatch.setattr(
            save_tables,
            "fetch_extents",
            lambda *a: [(4, 100, 64), (4, 164, 64)],
        )
        predicates = save_tables.fetch_rowid_predicates(
            None, {}, "admin", "orders", num_partitions=2
        )
        first_start = save_tables.encode_rowid(OBJ, 4, 100, 0)
        first_end = save_tables.encode_rowid(OBJ, 4, 163, save_tables.ROWID_MAX_ROW)
        second_start = save_tables.encode_rowid(OBJ, 4, 164, 0)
        second_end = save_tables.encode_rowid(OBJ, 4, 227, save_tables.ROWID_MAX_ROW)
        assert predicates == [
            f"ROWID BETWEEN '{first_start}' AND '{first_end}'",
            f"ROWID BETWEEN '{second_start}' AND '{second_end}'",
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
