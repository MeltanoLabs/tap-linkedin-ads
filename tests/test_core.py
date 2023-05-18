"""Tests standard tap features using the built-in SDK tests library."""

from singer_sdk.testing import get_tap_test_class

from tap_linkedin_ads.tap import TapLinkedInAds

SAMPLE_CONFIG = {
    "start_date": "2023-01-01T00:00:00Z",
}


TestTapLinkedInAds = get_tap_test_class(TapLinkedInAds, config=SAMPLE_CONFIG)
