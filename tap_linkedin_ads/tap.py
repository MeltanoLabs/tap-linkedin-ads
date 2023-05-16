"""LinkedInAds tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_linkedin_ads.client import LinkedInAdsStream
import tap_linkedin_ads.streams as streams

import datetime



class TapLinkedInAds(Tap):
    """LinkedInAds tap class."""

    name = "tap-linkedin-ads"

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
            description="Generated token, bearer auth",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            required=False,
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
            description="LinkedInAds API Version",
        ),
        th.Property(
            "accounts",
            th.StringType,
            description="LinkedInAds Account ID",
        ),
        th.Property(
            "campaign",
            th.StringType,
            description="LinkedInAds Campaign ID",
        ),
        th.Property(
            "owner",
            th.StringType,
            description="LinkedInAds Owner ID",
        ),

    ).to_dict()

    def discover_streams(self) -> list[LinkedInAdsStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """

        # TODO: RESOLVE SDK ENCODING ISSUE FOR CREATIVES STREAM: [LINK TO GITHUB ISSUE]
        return [
            streams.Accounts(self),
            streams.VideoAds(self),
            streams.AccountUsers(self),
            # streams.Creatives(self),
            streams.Campaigns(self),
            streams.CampaignGroups(self),
            streams.AdAnalyticsByCampaign(self),
            streams.AdAnalyticsByCreative(self),
        ]


if __name__ == "__main__":
    TapLinkedInAds.cli()
