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
