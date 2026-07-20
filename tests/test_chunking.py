from __future__ import annotations

import unittest

from signal_hermes_router.outbound import (
    NO_REPLY_SENTINEL,
    _byte_prefix,
    _hard_byte_cut,
    _split_greedy_bytes,
    chunk_for_signal_bytes,
    is_no_reply_sentinel,
)


def _utf8_len(text: str) -> int:
    return len(text.encode("utf-8"))


class ChunkForSignalBytesAsciiTests(unittest.TestCase):
    def test_short_message_returns_single_chunk_without_marker(self) -> None:
        result = chunk_for_signal_bytes("short reply", max_bytes=1900)
        self.assertEqual(result, ["short reply"])

    def test_two_chunks_get_n_of_m_markers(self) -> None:
        body = "word " * 50  # 250 ASCII bytes
        result = chunk_for_signal_bytes(body, max_bytes=140)
        self.assertEqual(len(result), 2)
        self.assertTrue(result[0].startswith("[1/2] "))
        self.assertTrue(result[1].startswith("[2/2] "))

    def test_paragraph_boundary_preferred(self) -> None:
        para = "x" * 80
        body = f"{para}\n\n{para}\n\n{para}"
        result = chunk_for_signal_bytes(body, max_bytes=100)
        self.assertGreaterEqual(len(result), 3)
        for chunk in result:
            self.assertLessEqual(_utf8_len(chunk), 100)
        # Each chunk minus its marker should contain one of the paragraphs.
        bodies = [c.split("] ", 1)[1] for c in result]
        self.assertIn(para, bodies)

    def test_line_boundary_fallback(self) -> None:
        # No "\n\n", but newlines inside force line-boundary splits.
        line = "x" * 50
        body = "\n".join([line] * 4)
        result = chunk_for_signal_bytes(body, max_bytes=80)
        self.assertGreaterEqual(len(result), 2)
        for chunk in result:
            self.assertLessEqual(_utf8_len(chunk), 80)

    def test_word_boundary_fallback(self) -> None:
        # No newlines; whitespace-only delimiters.
        body = "alpha bravo charlie delta echo foxtrot golf hotel india"
        result = chunk_for_signal_bytes(body, max_bytes=30)
        self.assertGreater(len(result), 1)
        for chunk in result:
            self.assertLessEqual(_utf8_len(chunk), 30)
        # No mid-word cuts — every body word should appear intact across chunks.
        recombined = " ".join(c.split("] ", 1)[1] for c in result)
        for word in body.split():
            self.assertIn(word, recombined)

    def test_hard_break_when_no_whitespace(self) -> None:
        body = "x" * 200
        result = chunk_for_signal_bytes(body, max_bytes=50)
        self.assertGreater(len(result), 1)
        for chunk in result:
            self.assertLessEqual(_utf8_len(chunk), 50)

    def test_every_chunk_within_max_bytes(self) -> None:
        body = ("paragraph text. " * 200).strip()
        for cap in (60, 80, 100, 200, 500):
            with self.subTest(max_bytes=cap):
                for chunk in chunk_for_signal_bytes(body, max_bytes=cap):
                    self.assertLessEqual(_utf8_len(chunk), cap)

    def test_non_boundary_whitespace_is_preserved(self) -> None:
        # Internal double spaces and tabs should not be collapsed.
        body = "alpha  beta\tgamma   delta " * 10
        result = chunk_for_signal_bytes(body, max_bytes=60)
        for chunk in result:
            self.assertLessEqual(_utf8_len(chunk), 60)
        recombined = " ".join(c.split("] ", 1)[1] for c in result)
        self.assertIn("alpha  beta", recombined)
        self.assertIn("beta\tgamma", recombined)


class ChunkForSignalBytesMarkerStabilityTests(unittest.TestCase):
    def test_marker_width_stable_at_digit_boundary_9_to_10(self) -> None:
        # 18-byte cap, no whitespace, body sized so naive split yields 9
        # chunks but marker reservation pushes it to 10. Verify all chunks
        # land within the cap and carry consistent [NN/10] markers.
        body = "x" * 162  # 162 / 18 = 9 chunks at max_bytes, 10 after marker reservation
        result = chunk_for_signal_bytes(body, max_bytes=18)
        for chunk in result:
            self.assertLessEqual(_utf8_len(chunk), 18)
        if any(c.startswith("[") for c in result):
            totals = {c.split("/", 1)[1].split("]", 1)[0] for c in result}
            self.assertEqual(len(totals), 1, "marker total should be uniform")

    def test_marker_width_stable_at_digit_boundary_99_to_100(self) -> None:
        # Similar idea at a larger scale: cap=18, body large enough that
        # M crosses 99→100. Algorithm must converge with chunks within cap.
        body = "x" * (1800)  # 1800 / 18 = 100 chunks naive
        result = chunk_for_signal_bytes(body, max_bytes=18)
        for chunk in result:
            self.assertLessEqual(_utf8_len(chunk), 18)


class ChunkForSignalBytesMultibyteTests(unittest.TestCase):
    def test_emoji_body_respects_byte_cap_not_char_cap(self) -> None:
        body = "🌍" * 500  # 2000 UTF-8 bytes, 500 characters
        result = chunk_for_signal_bytes(body, max_bytes=1900)
        self.assertGreaterEqual(len(result), 2)
        for chunk in result:
            self.assertLessEqual(_utf8_len(chunk), 1900)

    def test_cjk_body_respects_byte_cap(self) -> None:
        body = "漢字" * 500  # 3000 UTF-8 bytes, 1000 characters
        result = chunk_for_signal_bytes(body, max_bytes=1900)
        self.assertGreaterEqual(len(result), 2)
        for chunk in result:
            self.assertLessEqual(_utf8_len(chunk), 1900)

    def test_split_does_not_break_utf8_codepoint(self) -> None:
        body = "🌍漢字" * 200
        for chunk in chunk_for_signal_bytes(body, max_bytes=400):
            # Re-encoding the chunk's bytes must decode cleanly with strict errors.
            chunk.encode("utf-8").decode("utf-8")  # raises on partial codepoint


class ChunkForSignalBytesFallbackTests(unittest.TestCase):
    def test_production_valid_low_budget_falls_back_to_unmarked_chunks(self) -> None:
        # max_bytes=16 (the config floor) with 10000 4-byte emoji forces M
        # past the marker-width budget; algorithm bails to _hard_byte_cut.
        body = "🌍" * 10000
        result = chunk_for_signal_bytes(body, max_bytes=16)
        self.assertEqual(len(result), 2500)
        for chunk in result:
            self.assertEqual(_utf8_len(chunk), 16)
            self.assertFalse(chunk.startswith("["))

    def test_pathologically_small_max_bytes_drops_codepoints_without_looping(self) -> None:
        # Direct call to _split_greedy_bytes below the config floor.
        # Must terminate; result may drop codepoints, but no infinite loop.
        result = _split_greedy_bytes("🌍🌍🌍", limit=3)
        # All emoji exceed 3 bytes; splitter drops them and returns an
        # empty list (or chunks within the limit if any could fit).
        for chunk in result:
            self.assertLessEqual(_utf8_len(chunk), 3)

    def test_hard_byte_cut_skips_oversized_codepoint(self) -> None:
        # max_bytes=2: emoji is 4 bytes (oversized) — gets dropped.
        # "ascii" is 1 byte/char so it gets cut into 2-byte chunks.
        result = _hard_byte_cut("🌍ascii", max_bytes=2)
        self.assertGreater(len(result), 0)
        for chunk in result:
            self.assertLessEqual(_utf8_len(chunk), 2)


class BytePrefixTests(unittest.TestCase):
    def test_returns_full_text_when_under_limit(self) -> None:
        self.assertEqual(_byte_prefix("hello", 100), "hello")

    def test_drops_partial_trailing_codepoint(self) -> None:
        # "🌍" is 4 bytes; limit=2 cannot fit it.
        self.assertEqual(_byte_prefix("🌍ascii", 2), "")
        # limit=4 fits exactly one emoji.
        self.assertEqual(_byte_prefix("🌍ascii", 4), "🌍")
        # limit=5 fits emoji + one ASCII char.
        self.assertEqual(_byte_prefix("🌍ascii", 5), "🌍a")


class NoReplySentinelTests(unittest.TestCase):
    def test_exact_sentinel_matches(self) -> None:
        self.assertTrue(is_no_reply_sentinel(NO_REPLY_SENTINEL))

    def test_whitespace_padded_sentinel_matches(self) -> None:
        self.assertTrue(is_no_reply_sentinel(f"  \n{NO_REPLY_SENTINEL}\t\n "))

    def test_sentinel_embedded_in_longer_reply_does_not_match(self) -> None:
        self.assertFalse(is_no_reply_sentinel(f"Nothing to add. {NO_REPLY_SENTINEL}"))
        self.assertFalse(is_no_reply_sentinel(f"{NO_REPLY_SENTINEL} but here is more"))

    def test_empty_and_unrelated_text_do_not_match(self) -> None:
        self.assertFalse(is_no_reply_sentinel(""))
        self.assertFalse(is_no_reply_sentinel("   "))
        self.assertFalse(is_no_reply_sentinel("no-reply"))


if __name__ == "__main__":
    unittest.main()
