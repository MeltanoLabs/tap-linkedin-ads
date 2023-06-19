"""Tests standard tap features using the built-in SDK tests library."""
from __future__ import annotations

from singer_sdk.testing import SuiteConfig, get_tap_test_class

from tap_linkedin_ads.tap import TapLinkedInAds

SAMPLE_CONFIG = {
    "start_date": "2023-01-01T00:00:00Z",
}


TestTapLinkedInAds = get_tap_test_class(
    TapLinkedInAds,
    config=SAMPLE_CONFIG,
    suite_config=SuiteConfig(
        ignore_no_records_for_streams=["video_ads"],
    ),
)
