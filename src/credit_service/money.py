"""Money handling: amounts are Decimals with exactly two decimal places, never floats."""

from decimal import Decimal, InvalidOperation

CENT = Decimal("0.01")
MAX_CREDIT = Decimal("1000000.00")


class InvalidAmountError(ValueError):
    pass


def parse_amount(raw: object) -> Decimal:
    """Parse a client-supplied credit amount.

    Accepts decimal strings ("50.00") and JSON numbers. Floats go through their
    shortest repr, so 0.1 becomes Decimal("0.1") rather than its binary approximation.
    """
    if isinstance(raw, bool) or not isinstance(raw, str | int | float | Decimal):
        raise InvalidAmountError("amount must be a decimal string or number")
    try:
        value = Decimal(repr(raw)) if isinstance(raw, float) else Decimal(raw)
    except InvalidOperation:
        raise InvalidAmountError("amount must be a decimal string or number") from None

    if not value.is_finite():
        raise InvalidAmountError("amount must be a finite number")
    if value <= 0:
        raise InvalidAmountError("amount must be greater than zero")
    if value > MAX_CREDIT:
        raise InvalidAmountError(f"amount must not exceed {MAX_CREDIT}")
    if value != value.quantize(CENT):
        raise InvalidAmountError("amount must have at most two decimal places")
    return value.quantize(CENT)


def format_amount(value: Decimal) -> str:
    return f"{value.quantize(CENT):f}"
