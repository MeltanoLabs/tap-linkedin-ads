"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import get_standard_tap_tests

from tap_linkedin.tap import TapLinkedIn

import os
from dotenv import load_dotenv

load_dotenv()

linkedin_config = {
    "client_secret":os.getenv("TAP_LINKEDIN_CLIENT_SECRET"),
    "access_token":os.getenv("TAP_LINKEDIN_ACCESS_TOKEN"),
    "refresh_token":os.getenv("TAP_LINKEDIN_REFRESH_TOKEN"),
    "client_id":os.getenv("TAP_LINKEDIN_CLIENT_ID"),
    "account_id":os.getenv("TAP_LINKEDIN_ACCOUNTS"),
    "user_agent":os.getenv("TAP_LINKEDIN_USER_AGENT"),
    "start_date":os.getenv("TAP_LINKEDIN_START_DATE"),
    "end_date":os.getenv("TAP_LINKEDIN_END_DATE"),
    "linkedin_version":"202207",
    "x-restli-protocol-version":"1.0.0",
    "owner":os.getenv("TAP_LINKEDIN_OWNER"),
    "campaign":os.getenv("TAP_LINKEDIN_CAMPAIGN")
}


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(
        TapLinkedIn,
        config=linkedin_config
    )
    for test in tests:
        test()
