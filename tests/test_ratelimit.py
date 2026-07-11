from __future__ import annotations

import unittest

from signal_hermes_router.ratelimit import TokenBucket


class TokenBucketTests(unittest.TestCase):
    def test_bucket_starts_full_and_drains(self) -> None:
        bucket = TokenBucket(capacity=2, refill_per_second=1)

        self.assertTrue(bucket.try_acquire(1_000))
        self.assertTrue(bucket.try_acquire(1_000))
        self.assertFalse(bucket.try_acquire(1_000))

    def test_partial_refill_grants_after_enough_elapsed(self) -> None:
        bucket = TokenBucket(capacity=1, refill_per_second=0.5)
        self.assertTrue(bucket.try_acquire(0))

        # 1 second at 0.5 tokens/second is not yet a whole token.
        self.assertFalse(bucket.try_acquire(1_000))
        # 2 total elapsed seconds mint the token.
        self.assertTrue(bucket.try_acquire(2_000))

    def test_refill_caps_at_capacity(self) -> None:
        bucket = TokenBucket(capacity=2, refill_per_second=100)
        self.assertTrue(bucket.try_acquire(0))
        self.assertTrue(bucket.try_acquire(0))

        # A long idle stretch refills to capacity, not beyond.
        self.assertTrue(bucket.try_acquire(3_600_000))
        self.assertTrue(bucket.try_acquire(3_600_000))
        self.assertFalse(bucket.try_acquire(3_600_000))

    def test_backwards_clock_does_not_mint_tokens(self) -> None:
        bucket = TokenBucket(capacity=1, refill_per_second=1)
        self.assertTrue(bucket.try_acquire(10_000))

        self.assertFalse(bucket.try_acquire(5_000))
        # Refill resumes from the last observed time, not the stepped-back one.
        self.assertTrue(bucket.try_acquire(11_000))

    def test_constructor_rejects_invalid_parameters(self) -> None:
        with self.assertRaises(ValueError):
            TokenBucket(capacity=0, refill_per_second=1)
        with self.assertRaises(ValueError):
            TokenBucket(capacity=1, refill_per_second=0)


if __name__ == "__main__":
    unittest.main()
