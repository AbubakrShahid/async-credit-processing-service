from decimal import Decimal

import pytest

from credit_service.money import InvalidAmountError, format_amount, parse_amount


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("50", "50.00"),
        ("50.5", "50.50"),
        ("50.100", "50.10"),  # trailing zeros are not extra precision
        (0.1, "0.10"),  # float goes through repr, not its binary value
        (25, "25.00"),
        ("1000000.00", "1000000.00"),
    ],
)
def test_parses_valid_amounts(raw: object, expected: str) -> None:
    assert parse_amount(raw) == Decimal(expected)
    assert format_amount(parse_amount(raw)) == expected


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        ("0", "greater than zero"),
        ("-5", "greater than zero"),
        ("10.001", "two decimal places"),
        ("1000000.01", "must not exceed"),
        ("NaN", "finite"),
        ("Infinity", "finite"),
        ("abc", "decimal string or number"),
        (True, "decimal string or number"),
        (None, "decimal string or number"),
    ],
)
def test_rejects_invalid_amounts(raw: object, message: str) -> None:
    with pytest.raises(InvalidAmountError, match=message):
        parse_amount(raw)
