"""Plan 136 Stage 2: the shared Cloudflare-interstitial marker set.

Two callers classify challenge pages for different reasons and must not drift:
`processing` decides whether a stored page is a genuine block, and `scraper`
decides what to count in `cartracker_solver_requests_total`.
"""
import pytest

from shared.challenge import CHALLENGE_TITLE_RE, html_title, title_looks_like_challenge


class TestHtmlTitle:
    def test_reads_a_title(self):
        assert html_title(b"<html><head><title>Hello</title></head></html>") == "Hello"

    def test_collapses_whitespace_and_is_case_insensitive_about_the_tag(self):
        assert html_title(b"<HTML><TITLE>a\n  b</TITLE>") == "a b"

    def test_missing_or_empty_input_is_none(self):
        assert html_title(b"<html><body>no title</body></html>") is None
        assert html_title(b"") is None
        assert html_title(None) is None

    def test_undecodable_bytes_do_not_raise(self):
        assert html_title(b"\xff\xfe<title>ok</title>") == "ok"

    def test_only_the_first_4kb_is_read(self):
        """The title is in the head. Scanning whole pages would put this on the
        scrape hot path for no gain."""
        buried = b"<html><body>" + b"x" * 5000 + b"<title>too late</title></body>"
        assert html_title(buried) is None

    def test_a_long_title_is_bounded(self):
        assert len(html_title(b"<title>" + b"z" * 500 + b"</title>")) == 200


class TestChallengeTitles:
    @pytest.mark.parametrize(
        "title",
        [
            "Just a moment...",
            "just a moment",
            "Attention Required! | Cloudflare",
            "Checking your browser before accessing",
        ],
    )
    def test_interstitial_titles_match(self, title):
        assert title_looks_like_challenge(title)

    @pytest.mark.parametrize(
        "title",
        [
            "New 2026 Toyota RAV4 XLE for sale",
            "Cars for Sale | Cars.com",
            "",
            None,
        ],
    )
    def test_real_titles_and_absent_titles_do_not_match(self, title):
        """Validated against known-good titles, not only against known-bad ones.
        A marker that also matches real pages is not a discriminator."""
        assert not title_looks_like_challenge(title)

    def test_the_regex_is_the_one_processing_uses(self):
        """Guards the extraction: parse_detail_page imports this object rather
        than defining its own copy, so a marker added for the scraper's counter
        reaches the block classifier too."""
        from processing.processors.parse_detail_page import CHALLENGE_TITLE_RE as theirs

        assert theirs is CHALLENGE_TITLE_RE
