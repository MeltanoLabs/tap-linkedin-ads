"""Tests standard tap features using the built-in SDK tests library."""

from singer_sdk.testing import get_standard_tap_tests

from tap_linkedin.tap import TapLinkedIn

SAMPLE_CONFIG = {
    "client_secret": "",
    "access_token": "",
    "refresh_token": "",
    "client_id": "",
    "account_id": "",
    "user_agent": "meltano",
    "start_date": "2023-01-01T00:00:00Z",
    "linkedin_version": "202207",
    "owner": "",
    "campaign": "",
}


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(TapLinkedIn, config=SAMPLE_CONFIG)
    for test in tests:
        test()
