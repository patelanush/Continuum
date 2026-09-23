from decimal import Decimal

import pytest
from checkout.pricing import calculate_discount


def test_combined_discounts_apply_sequentially() -> None:
    assert calculate_discount(Decimal("100.00"), Decimal("0.20"), Decimal("0.10")) == Decimal(
        "28.00"
    )


def test_100_percent_promotion_leaves_nothing_for_loyalty() -> None:
    assert calculate_discount(Decimal("25.00"), Decimal("1.00"), Decimal("0.50")) == Decimal(
        "25.00"
    )


def test_no_discount() -> None:
    assert calculate_discount(Decimal("19.99"), Decimal("0"), Decimal("0")) == Decimal("0.00")


@pytest.mark.parametrize("rate", [Decimal("-0.01"), Decimal("1.01")])
def test_invalid_rate(rate: Decimal) -> None:
    with pytest.raises(ValueError):
        calculate_discount(Decimal("10.00"), rate, Decimal("0"))


def test_negative_subtotal() -> None:
    with pytest.raises(ValueError):
        calculate_discount(Decimal("-1.00"), Decimal("0"), Decimal("0"))
