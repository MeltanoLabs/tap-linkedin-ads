"""LinkedInAds tap class."""

from __future__ import annotations

import datetime
import typing as t

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_linkedin_ads import streams

if t.TYPE_CHECKING:
    from tap_linkedin_ads.streams import LinkedInAdsStream

NOW = datetime.datetime.now(tz=datetime.timezone.utc)


class TapLinkedInAds(Tap):
    """Singer tap for extracting data from the LinkedIn Ads Marketing API."""

    name = "tap-linkedin-ads"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "access_token",
            th.StringType,
            description="The token to authenticate against the API service",
        ),
        # OAuth
        th.Property(
            "oauth_credentials",
            th.ObjectType(
                th.Property(
                    "refresh_token",
                    th.StringType,
                    description="LinkedIn Ads Refresh Token",
                ),
                th.Property(
                    "client_id",
                    th.StringType,
                    description="LinkedIn Ads Client ID",
                ),
                th.Property(
                    "client_secret",
                    th.StringType,
                    secret=True,
                    description="LinkedIn Ads Client Secret",
                ),
            ),
            description="LinkedIn Ads OAuth Credentials",
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
            default=NOW.isoformat(),
            description="The latest record date to sync",
        ),
        th.Property(
            "user_agent",
            th.StringType,
            default="tap-linkedin-ads <api_user_email@your_company.com>",
            description="API ID",
        ),
        th.Property(
            "accounts",
            th.StringType,
            required=True,
            description="LinkedInAds Account ID",
        ),
        th.Property(
            "campaign",
            th.StringType,
            required=True,
            description="LinkedInAds Campaign ID",
        ),
        th.Property(
            "owner",
            th.StringType,
            required=True,
            description="LinkedInAds Owner ID",
        ),
        th.Property(
            "campaign_group",
            th.StringType,
            required=True,
            description="LinkedInAds Campaign Group ID. Used for the campaign_group stream",
        ),
        th.Property(
            "creative",
            th.StringType,
            required=True,
            description="LinkedInAds Creative ID. Used for the creative stream",
        ),
    ).to_dict()

    def discover_streams(self) -> list[LinkedInAdsStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.Accounts(self),
            streams.VideoAds(self),
            streams.AccountUsers(self),
            streams.Creatives(self),
            streams.Campaigns(self),
            streams.CampaignGroups(self),
            streams.AdAnalyticsByCampaign(self),
            streams.AdAnalyticsByCreative(self),
        ]


if __name__ == "__main__":
    TapLinkedInAds.cli()
