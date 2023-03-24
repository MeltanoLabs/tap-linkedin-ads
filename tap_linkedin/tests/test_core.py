"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import get_standard_tap_tests

from tap_linkedin.tap import (TapLinkedIn)

from dotenv import load_dotenv

# TODO: Remove this:

# load_dotenv(".env")
# LinkedInAccounts = os.getenv("TAP_LINKEDIN_ACCOUNTS")
# LinkedInOwner = os.getenv("TAP_LINKEDIN_OWNER")
# LinkedInCampaign = os.getenv("TAP_LINKEDIN_CAMPAIGN")
# StartDateMonth = os.getenv("TAP_LINKEDIN_START_DATE_MONTH")
# StartDateDay = os.getenv("TAP_LINKEDIN_START_DATE_DAY")
# StartDateYear = os.getenv("TAP_LINKEDIN_START_DATE_YEAR")
# EndDateMonth = os.getenv("TAP_LINKEDIN_END_DATE_MONTH")
# EndDateDay = os.getenv("TAP_LINKEDIN_END_DATE_DAY")
# EndDateYear = os.getenv("TAP_LINKEDIN_END_DATE_YEAR")

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    # TODO: Initialize minimal tap config
}


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(
        TapLinkedIn,
        config=SAMPLE_CONFIG,
    )
    for test in tests:
        test()


# TODO: Create additional tests as appropriate for your tap.
