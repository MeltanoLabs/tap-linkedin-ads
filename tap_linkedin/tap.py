"""LinkedIn tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_linkedin.streams import LinkedInStream
import tap_linkedin.streams as streams

import datetime

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
            "api_version",
            th.StringType,
            default="202207",
            description="LinkedIn API Version",
        ),
        th.Property(
            "accounts",
            th.StringType,
            description="LinkedIn Account ID",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.LinkedInStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """

        #TODO: RESOLVE SDK ENCODING ISSUE FOR CREATIVES STREAM: [LINK TO GITHUB ISSUE]
        return [
            streams.accounts(self),
            streams.videoAds(self),
            streams.accountUsers(self),
            # streams.creatives(self),
            streams.campaigns(self),
            streams.campaignGroups(self),
            streams.adAnalyticsByCampaign(self),
            streams.adAnalyticsByCreative(self),
        ]


if __name__ == "__main__":
    TapLinkedIn.cli()
