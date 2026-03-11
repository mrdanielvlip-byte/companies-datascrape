"""Tests for entity resolution (name normalization and similarity)."""
import pytest
from app.normalization.entity_resolution import (
    normalize_name,
    name_similarity,
)


class TestNormalizeName:
    def test_basic(self):
        assert normalize_name("SMITH, John") == "john smith"

    def test_strips_titles(self):
        assert normalize_name("Mr John Smith") == "john smith"
        assert normalize_name("Dr. Jane Doe CBE") == "doe jane"

    def test_order_invariant(self):
        assert normalize_name("Smith John") == normalize_name("John Smith")

    def test_strips_accents(self):
        result = normalize_name("José García")
        assert "jose" in result
        assert "garcia" in result

    def test_empty(self):
        assert normalize_name("") == ""
        assert normalize_name(None) == ""

    def test_hyphenated(self):
        result = normalize_name("Mary-Jane Watson")
        assert "mary-jane" in result
        assert "watson" in result

    def test_complex_titles(self):
        result = normalize_name("Prof. Sir James Wilson OBE FCA")
        assert "james" in result
        assert "wilson" in result
        assert "prof" not in result
        assert "sir" not in result


class TestNameSimilarity:
    def test_identical(self):
        assert name_similarity("john smith", "john smith") == 1.0

    def test_completely_different(self):
        sim = name_similarity("john smith", "xyz abc")
        assert sim < 0.3

    def test_similar_names(self):
        sim = name_similarity("john smith", "john smyth")
        assert sim > 0.6

    def test_empty(self):
        assert name_similarity("", "john") == 0.0
        assert name_similarity("john", "") == 0.0


class TestNormalizeFilings:
    """Tests for filing normalization functions."""

    def test_normalize_filing_record(self):
        from app.normalization.normalize_filings import normalize_filing_record

        raw = {
            "transaction_id": "TX-001",
            "date": "2024-06-15",
            "category": "accounts",
            "type": "AA",
            "description": "accounts for period ending {made_up_date}",
            "description_values": {"made_up_date": "2024-03-31"},
        }
        result = normalize_filing_record(raw, "12345678")
        assert result is not None
        assert result["transaction_id"] == "TX-001"
        assert result["category"] == "accounts"
        assert "2024-03-31" in result["description"]

    def test_skip_no_transaction_id(self):
        from app.normalization.normalize_filings import normalize_filing_record

        raw = {"date": "2024-06-15", "category": "accounts", "type": "AA"}
        result = normalize_filing_record(raw, "12345678")
        assert result is None


class TestNormalizePsc:
    """Tests for PSC normalization functions."""

    def test_normalize_individual_psc(self):
        from app.normalization.normalize_psc import normalize_psc_record

        record = {
            "company_number": "12345678",
            "kind": "individual-person-with-significant-control",
            "name_elements": {
                "forename": "John",
                "surname": "Smith",
            },
            "date_of_birth": {"month": 6, "year": 1970},
            "natures_of_control": [
                "ownership-of-shares-75-to-100-percent",
            ],
            "notified_on": "2020-01-15",
        }
        result = normalize_psc_record(record)
        assert result is not None
        assert result["psc_name"] == "John Smith"
        assert result["psc_kind"] == "individual"
        assert result["birth_month"] == 6
        assert result["birth_year"] == 1970

    def test_normalize_corporate_psc(self):
        from app.normalization.normalize_psc import normalize_psc_record

        record = {
            "company_number": "12345678",
            "kind": "corporate-entity-person-with-significant-control",
            "name": "HOLDING CO LTD",
            "notified_on": "2019-03-01",
        }
        result = normalize_psc_record(record)
        assert result is not None
        assert result["psc_name"] == "HOLDING CO LTD"
        assert result["psc_kind"] == "corporate"

    def test_skip_unknown_name(self):
        from app.normalization.normalize_psc import normalize_psc_record

        record = {"company_number": "12345678", "kind": "unknown"}
        result = normalize_psc_record(record)
        assert result is None
