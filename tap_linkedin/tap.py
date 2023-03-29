"""LinkedIn tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_linkedin.streams import (LinkedInStream)
import tap_linkedin.streams as streams

import datetime
import os
from dotenv import load_dotenv

load_dotenv(".env")
LinkedInAccounts = os.getenv("TAP_LINKEDIN_ACCOUNTS")
LinkedInVersion = os.getenv("TAP_LINKEDIN_LINKEDIN_VERSION")

STREAM_TYPES = [LinkedInStream]

class TapLinkedIn(Tap):
    """LinkedIn tap class."""

    name = "tap-linkedin-sdk"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "access_token",
            th.StringType,
            required=True,
            description="The token to authenticate against the API service",
        ),
        th.Property(
            "refresh_token",
            th.StringType,
            required=True,
            description="Generated token, bearer auth",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            required=True,
            description="The earliest record date to sync",
        ),
        th.Property(
            "end_date",
            th.DateTimeType,
            required=False,
            default=str(datetime.datetime.utcnow()),
            description="The latest record date to sync",
        ),
        th.Property(
            "client_secret",
            th.StringType,
            required=True,
            description="client secret key",
        ),
        th.Property(
            "user_agent",
            th.StringType,
            default="tap-linkedin-ads <api_user_email@your_company.com>",
            description="API ID",
        ),
        th.Property(
            "LinkedIn-Version",
            th.StringType,
            default=LinkedInVersion,
            description="LinkedIn API Version",
        ),
        th.Property(
            "accounts",
            th.StringType,
            default=LinkedInAccounts,
            description="LinkedIn Account ID",
        )
    ).to_dict()

    def discover_streams(self) -> list[streams.LinkedInStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """

        return [
            streams.Accounts(self),
            streams.VideoAds(self),
            streams.AccountUsers(self),
            #streams.Creatives(self),
            streams.Campaigns(self),
            streams.CampaignGroups(self),
            streams.AdAnalyticsByCreative(self),
            streams.AdAnalyticsByCampaign(self)
        ]


if __name__ == "__main__":
    TapLinkedIn.cli()