# Task

The checkout discount calculation over-discounts orders that have both a promotional discount and a loyalty discount. Loyalty savings should be calculated on the remaining subtotal *after* the promotion, not the original subtotal. Preserve the public function signature and cap the final discount at the original subtotal. Fix the implementation and run the tests.
