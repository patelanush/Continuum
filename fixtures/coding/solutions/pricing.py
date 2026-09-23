"""Checkout discount calculation."""

from decimal import Decimal


def calculate_discount(
    subtotal: Decimal,
    promotion_rate: Decimal,
    loyalty_rate: Decimal,
) -> Decimal:
    """Return the total discount, rounded to cents, without exceeding subtotal.

    Promotion applies first. Loyalty then applies to the amount still due.
    Rates are fractions between zero and one.
    """
    if subtotal < 0:
        raise ValueError("subtotal must be nonnegative")
    if not (0 <= promotion_rate <= 1 and 0 <= loyalty_rate <= 1):
        raise ValueError("discount rates must be between zero and one")

    promotion = subtotal * promotion_rate
    loyalty = (subtotal - promotion) * loyalty_rate
    discount = promotion + loyalty
    return min(subtotal, discount).quantize(Decimal("0.01"))
