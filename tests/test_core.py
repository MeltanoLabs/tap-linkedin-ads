"""Tests standard tap features using the built-in SDK tests library."""

from singer_sdk.testing import get_standard_tap_tests

from tap_linkedin_ads.tap import TapLinkedInAds

SAMPLE_CONFIG = {

    "start_date": "2023-01-01T00:00:00Z",

}


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(TapLinkedInAds, config=SAMPLE_CONFIG)
    for test in tests:
        test()
