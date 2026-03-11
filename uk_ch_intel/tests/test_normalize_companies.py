"""Tests for company normalization."""
import pandas as pd
import pytest
from app.normalization.normalize_companies import (
    normalize_company_chunk,
    normalize_api_company,
    _parse_sic_codes,
    _build_registered_address,
    _parse_date,
)


class TestParseSicCodes:
    def test_standard_sic(self):
        row = pd.Series({"sic_1": "43210 - Electrical installation", "sic_2": None, "sic_3": None, "sic_4": None})
        assert _parse_sic_codes(row) == ["43210"]

    def test_multiple_sics(self):
        row = pd.Series({
            "sic_1": "43210 - Electrical installation",
            "sic_2": "71121 - Engineering activities",
            "sic_3": None,
            "sic_4": None,
        })
        assert _parse_sic_codes(row) == ["43210", "71121"]

    def test_no_sics(self):
        row = pd.Series({"sic_1": None, "sic_2": None, "sic_3": None, "sic_4": None})
        assert _parse_sic_codes(row) is None


class TestParseDate:
    def test_uk_format(self):
        result = _parse_date("15/06/2020")
        assert str(result) == "2020-06-15"

    def test_iso_format(self):
        result = _parse_date("2020-06-15")
        assert str(result) == "2020-06-15"

    def test_empty(self):
        assert _parse_date(None) is None
        assert _parse_date("") is None
        assert _parse_date(float("nan")) is None


class TestBuildRegisteredAddress:
    def test_full_address(self):
        row = pd.Series({
            "address_line_1": "123 High Street",
            "address_line_2": "Suite 4",
            "post_town": "London",
            "county": "Greater London",
            "country": "England",
        })
        result = _build_registered_address(row)
        assert result == "123 High Street, Suite 4, London, Greater London, England"

    def test_partial_address(self):
        row = pd.Series({
            "address_line_1": "123 High Street",
            "address_line_2": None,
            "post_town": "London",
            "county": None,
            "country": None,
        })
        result = _build_registered_address(row)
        assert result == "123 High Street, London"


class TestNormalizeCompanyChunk:
    def test_basic_chunk(self):
        df = pd.DataFrame([{
            "company_number": "12345678",
            "company_name": "TEST LTD",
            "company_status": "Active",
            "company_type": "ltd",
            "jurisdiction": "england-wales",
            "incorporation_date": "01/01/2010",
            "dissolution_date": None,
            "address_line_1": "1 Test St",
            "address_line_2": None,
            "post_town": "London",
            "county": None,
            "country": "England",
            "postal_code": "EC1A 1BB",
            "accounts_next_due": None,
            "accounts_last_made_up_to": None,
            "confirmation_statement_next_due": None,
            "confirmation_statement_last_made_up_to": None,
            "sic_1": "62020 - Information technology consultancy",
            "sic_2": None,
            "sic_3": None,
            "sic_4": None,
        }])
        result = normalize_company_chunk(df, "test.csv")
        assert len(result) == 1
        assert result[0]["company_number"] == "12345678"
        assert result[0]["sic_codes"] == ["62020"]
        assert "London" in result[0]["registered_address"]

    def test_skip_empty_company_number(self):
        df = pd.DataFrame([{
            "company_number": "",
            "company_name": "GHOST LTD",
            "company_status": "Active",
            "company_type": "ltd",
        }])
        result = normalize_company_chunk(df)
        assert len(result) == 0

    def test_pads_company_number(self):
        df = pd.DataFrame([{
            "company_number": "1234",
            "company_name": "SHORT LTD",
            "company_status": "Active",
            "company_type": "ltd",
        }])
        result = normalize_company_chunk(df)
        assert result[0]["company_number"] == "00001234"


class TestNormalizeApiCompany:
    def test_api_response(self):
        payload = {
            "company_number": "12345678",
            "company_name": "API TEST LTD",
            "company_status": "active",
            "type": "ltd",
            "jurisdiction": "england-wales",
            "date_of_creation": "2010-01-01",
            "registered_office_address": {
                "address_line_1": "1 API Street",
                "locality": "London",
                "postal_code": "SW1A 1AA",
            },
            "sic_codes": ["62020"],
            "accounts": {"next_due": "2026-06-30"},
        }
        result = normalize_api_company(payload)
        assert result["company_number"] == "12345678"
        assert result["company_name"] == "API TEST LTD"
        assert result["source"] == "api"
        assert "1 API Street" in result["registered_address"]
