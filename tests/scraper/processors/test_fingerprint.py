"""Unit tests for processors/fingerprint.py"""
from scraper.processors.fingerprint import (
    _CHROME_VERSIONS,
    _VIEWPORTS,
    ZIP_POOL_LOCAL,
    ZIP_POOL_NATIONAL,
    human_delay,
    random_profile,
    random_zip,
)


class TestRandomProfile:
    def test_returns_required_keys(self):
        profile = random_profile()
        assert set(profile.keys()) == {"user_agent", "extra_http_headers", "viewport", "locale"}

    def test_user_agent_contains_valid_chrome_version(self):
        profile = random_profile()
        assert any(f"Chrome/{v}.0.0.0" in profile["user_agent"] for v in _CHROME_VERSIONS)

    def test_viewport_is_from_pool(self):
        profile = random_profile()
        vp = profile["viewport"]
        assert (vp["width"], vp["height"]) in _VIEWPORTS

    def test_sec_ch_headers_present(self):
        profile = random_profile()
        headers = profile["extra_http_headers"]
        assert "sec-ch-ua" in headers
        assert "sec-ch-ua-mobile" in headers
        assert "sec-ch-ua-platform" in headers

    def test_sec_ch_ua_mobile_is_not_mobile(self):
        # All profiles are desktop ("?0")
        assert random_profile()["extra_http_headers"]["sec-ch-ua-mobile"] == "?0"

    def test_sec_ch_ua_platform_is_windows(self):
        assert '"Windows"' in random_profile()["extra_http_headers"]["sec-ch-ua-platform"]

    def test_locale_is_en_us(self):
        assert random_profile()["locale"] == "en-US"

    def test_user_agent_is_windows_chrome(self):
        ua = random_profile()["user_agent"]
        assert "Windows NT 10.0" in ua
        assert "Chrome" in ua

    def test_sec_ch_ua_version_matches_user_agent(self):
        profile = random_profile()
        ua = profile["user_agent"]
        sec_ch = profile["extra_http_headers"]["sec-ch-ua"]
        # extract version from UA  e.g. "Chrome/133.0.0.0"
        import re
        version_match = re.search(r"Chrome/(\d+)", ua)
        assert version_match
        version = version_match.group(1)
        assert f'v="{version}"' in sec_ch


class TestRandomZip:
    def test_national_returns_national_pool_member(self):
        result = random_zip("national")
        assert result in ZIP_POOL_NATIONAL

    def test_local_returns_local_pool_member(self):
        result = random_zip("local")
        assert result in ZIP_POOL_LOCAL

    def test_unknown_scope_falls_back_to_national(self):
        # The function returns random.choice(ZIP_POOL_NATIONAL) for any non-"local" scope
        result = random_zip("global")
        assert result in ZIP_POOL_NATIONAL

    def test_empty_scope_falls_back_to_national(self):
        result = random_zip("")
        assert result in ZIP_POOL_NATIONAL

    def test_returns_string(self):
        assert isinstance(random_zip("local"), str)
        assert isinstance(random_zip("national"), str)


class TestHumanDelay:
    def test_returns_float(self):
        assert isinstance(human_delay(3), float)

    def test_page1_minimum_is_base_plus_early_bonus(self, mocker):
        # Patch random.uniform to return minimum values: 8 (base), 5 (early bonus)
        mocker.patch("scraper.processors.fingerprint.random.uniform", side_effect=[8.0, 5.0])
        mocker.patch(
            "scraper.processors.fingerprint.random.random", 
            return_value=0.5
        )  # no distraction
        result = human_delay(1)
        assert result == 13.0  # 8 + 5

    def test_page2_gets_early_page_bonus(self, mocker):
        mocker.patch("scraper.processors.fingerprint.random.uniform", side_effect=[10.0, 7.0])
        mocker.patch("scraper.processors.fingerprint.random.random", return_value=0.5)
        result = human_delay(2)
        assert result == 17.0  # 10 + 7

    def test_page3_no_early_page_bonus(self, mocker):
        mocker.patch("scraper.processors.fingerprint.random.uniform", return_value=10.0)
        mocker.patch("scraper.processors.fingerprint.random.random", return_value=0.5)
        result = human_delay(3)
        assert result == 10.0  # no early-page bonus

    def test_distraction_pause_branch_triggered(self, mocker):
        # random.random() < 0.10 → distraction branch fires
        mocker.patch("scraper.processors.fingerprint.random.random", return_value=0.05)
        # side_effect: [base, distraction_extra]  (no early bonus since page_num=5)
        mocker.patch("scraper.processors.fingerprint.random.uniform", side_effect=[10.0, 20.0])
        result = human_delay(5)
        assert result == 30.0  # 10 + 20

    def test_distraction_pause_not_triggered(self, mocker):
        mocker.patch("scraper.processors.fingerprint.random.random", return_value=0.5)
        mocker.patch("scraper.processors.fingerprint.random.uniform", return_value=12.0)
        result = human_delay(5)
        assert result == 12.0  # no distraction bonus

    def test_always_positive(self):
        for page_num in [1, 2, 3, 10, 50]:
            assert human_delay(page_num) > 0
