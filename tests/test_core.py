"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import get_tap_test_class

from tap_linkedin_ads.tap import TapLinkedInAds

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
}


# Run standard built-in tap tests from the SDK:
TestTapLinkedInAds = get_tap_test_class(
    tap_class=TapLinkedInAds,
    config=SAMPLE_CONFIG,
)
