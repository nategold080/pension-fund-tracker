"""Tests for normalization utilities."""

import pytest
from src.utils.normalization import (
    parse_dollar_amount,
    parse_percentage,
    parse_date,
    parse_multiple,
    parse_vintage_year,
    normalize_fund_name,
    normalize_gp_name,
)


class TestParseDollarAmount:
    """Tests for parse_dollar_amount."""

    def test_none_and_empty(self):
        assert parse_dollar_amount(None) is None
        assert parse_dollar_amount("") is None
        assert parse_dollar_amount("N/A") is None
        assert parse_dollar_amount("n/a") is None
        assert parse_dollar_amount("-") is None
        assert parse_dollar_amount("—") is None
        assert parse_dollar_amount("--") is None

    def test_billions(self):
        assert parse_dollar_amount("$1.2B") == 1200.0
        assert parse_dollar_amount("$1.2 billion") == 1200.0
        assert parse_dollar_amount("$2B") == 2000.0
        assert parse_dollar_amount("1.5b") == 1500.0

    def test_millions(self):
        assert parse_dollar_amount("$500M") == 500.0
        assert parse_dollar_amount("$500 million") == 500.0
        assert parse_dollar_amount("500m") == 500.0
        assert parse_dollar_amount("$1,200.5M") == 1200.5
        assert parse_dollar_amount("$100MM") == 100.0

    def test_raw_dollars(self):
        assert parse_dollar_amount("$45,000,000") == 45.0
        assert parse_dollar_amount("$1,200,000,000") == 1200.0
        assert parse_dollar_amount("45000000") == 45.0

    def test_context_in_millions(self):
        assert parse_dollar_amount("1,200.5", context_in_millions=True) == 1200.5
        assert parse_dollar_amount("500", context_in_millions=True) == 500.0
        assert parse_dollar_amount("45.3", context_in_millions=True) == 45.3

    def test_numeric_input(self):
        assert parse_dollar_amount(500.0, context_in_millions=True) == 500.0
        assert parse_dollar_amount(45000000) == 45.0
        assert parse_dollar_amount(45000000, context_in_millions=False) == 45.0

    def test_negative_parentheses(self):
        result = parse_dollar_amount("($500M)")
        assert result == -500.0

    def test_negative_sign(self):
        assert parse_dollar_amount("-$500M") == -500.0

    def test_thousands(self):
        result = parse_dollar_amount("$500K")
        assert result == pytest.approx(0.5)


class TestParsePercentage:
    """Tests for parse_percentage."""

    def test_none_and_empty(self):
        assert parse_percentage(None) is None
        assert parse_percentage("") is None
        assert parse_percentage("N/A") is None
        assert parse_percentage("-") is None
        assert parse_percentage("—") is None

    def test_percent_sign(self):
        assert parse_percentage("15.3%") == pytest.approx(0.153)
        assert parse_percentage("-2.1%") == pytest.approx(-0.021)
        assert parse_percentage("0%") == pytest.approx(0.0)
        assert parse_percentage("100%") == pytest.approx(1.0)

    def test_decimal_already(self):
        assert parse_percentage("0.153") == pytest.approx(0.153)
        assert parse_percentage("-0.021") == pytest.approx(-0.021)
        assert parse_percentage("0.5") == pytest.approx(0.5)

    def test_ambiguous_whole_number(self):
        # Numbers > 1 without % sign are treated as percentages
        assert parse_percentage("15.3") == pytest.approx(0.153)
        assert parse_percentage("-2.1") == pytest.approx(-0.021)

    def test_numeric_input(self):
        assert parse_percentage(15.3) == pytest.approx(0.153)
        assert parse_percentage(0.153) == pytest.approx(0.153)
        assert parse_percentage(-2.1) == pytest.approx(-0.021)

    def test_parentheses_negative(self):
        assert parse_percentage("(15.3%)") == pytest.approx(-0.153)


class TestParseDate:
    """Tests for parse_date."""

    def test_none_and_empty(self):
        assert parse_date(None) is None
        assert parse_date("") is None
        assert parse_date("N/A") is None
        assert parse_date("-") is None

    def test_iso_format(self):
        assert parse_date("2023-12-31") == "2023-12-31"
        assert parse_date("2023-06-30") == "2023-06-30"

    def test_us_format(self):
        assert parse_date("12/31/2023") == "2023-12-31"
        assert parse_date("6/30/2023") == "2023-06-30"

    def test_written_format(self):
        assert parse_date("December 31, 2023") == "2023-12-31"
        assert parse_date("June 30, 2023") == "2023-06-30"

    def test_quarter_notation(self):
        assert parse_date("Q4 2023") == "2023-12-31"
        assert parse_date("Q1 2023") == "2023-03-31"
        assert parse_date("Q2 2023") == "2023-06-30"
        assert parse_date("Q3 2023") == "2023-09-30"

    def test_fiscal_year(self):
        assert parse_date("FY 2023") == "2023-06-30"
        assert parse_date("FY2023") == "2023-06-30"

    def test_year_only(self):
        assert parse_date("2023") == "2023-12-31"

    def test_date_object(self):
        from datetime import date, datetime
        assert parse_date(date(2023, 12, 31)) == "2023-12-31"
        assert parse_date(datetime(2023, 12, 31, 15, 30)) == "2023-12-31"


class TestParseMultiple:
    """Tests for parse_multiple."""

    def test_none_and_empty(self):
        assert parse_multiple(None) is None
        assert parse_multiple("") is None
        assert parse_multiple("N/A") is None
        assert parse_multiple("-") is None

    def test_with_x_suffix(self):
        assert parse_multiple("1.5x") == 1.5
        assert parse_multiple("1.50X") == 1.5
        assert parse_multiple("2.1x") == 2.1

    def test_bare_number(self):
        assert parse_multiple("1.50") == 1.5
        assert parse_multiple("2.1") == 2.1
        assert parse_multiple("0.85") == 0.85

    def test_numeric_input(self):
        assert parse_multiple(1.5) == 1.5
        assert parse_multiple(2) == 2.0


class TestParseVintageYear:
    """Tests for parse_vintage_year."""

    def test_none_and_empty(self):
        assert parse_vintage_year(None) is None
        assert parse_vintage_year("") is None
        assert parse_vintage_year("N/A") is None
        assert parse_vintage_year("-") is None

    def test_integer(self):
        assert parse_vintage_year(2023) == 2023
        assert parse_vintage_year(2005) == 2005
        assert parse_vintage_year(1990) == 1990

    def test_string(self):
        assert parse_vintage_year("2023") == 2023
        assert parse_vintage_year("Vintage 2015") == 2015
        assert parse_vintage_year("FY 2020") == 2020

    def test_out_of_range(self):
        assert parse_vintage_year(1970) is None
        assert parse_vintage_year(2050) is None

    def test_float(self):
        assert parse_vintage_year(2023.0) == 2023


class TestNormalizeFundName:
    """Tests for normalize_fund_name."""

    def test_empty(self):
        assert normalize_fund_name("") == ""
        assert normalize_fund_name(None) == ""

    def test_remove_lp(self):
        assert normalize_fund_name("Blackstone Capital Partners VII, L.P.") == \
            "Blackstone Capital Partners VII"
        assert normalize_fund_name("KKR Americas XII Fund LP") == \
            "KKR Americas XII Fund"

    def test_remove_llc(self):
        assert normalize_fund_name("Apollo Global Management LLC") == \
            "Apollo Global Management"

    def test_expand_abbreviations(self):
        assert normalize_fund_name("BCP Fd VII") == "BCP Fund VII"
        assert normalize_fund_name("Blackstone Cap Prtrs VII") == \
            "Blackstone Capital Partners VII"

    def test_collapse_whitespace(self):
        assert normalize_fund_name("  Blackstone   Capital   Partners  VII  ") == \
            "Blackstone Capital Partners VII"


class TestNormalizeGpName:
    """Tests for normalize_gp_name - same as fund name normalization."""

    def test_basic(self):
        assert normalize_gp_name("Blackstone Group, L.P.") == "Blackstone Group"
        assert normalize_gp_name("KKR Mgmt LLC") == "KKR Management"
