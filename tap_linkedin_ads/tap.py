"""LinkedInAds tap class."""

from __future__ import annotations

import datetime

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_linkedin_ads import streams

NOW = datetime.datetime.now(tz=datetime.timezone.utc)


class TapLinkedInAds(Tap):
    """LinkedInAds tap class."""

    name = "tap-linkedin-ads"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "access_token",
            th.StringType,
            secret=True,
            description="The token to authenticate against the API service",
        ),
        # OAuth
        th.Property(
            "oauth_credentials",
            th.ObjectType(
                th.Property(
                    "refresh_token",
                    th.StringType,
                    secret=True,
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
    ).to_dict()

    def discover_streams(self) -> list[streams.LinkedInAdsStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.AccountsStream(self),
            streams.AccountUsersStream(self),
            streams.CampaignsStream(self),
            streams.CampaignGroupsStream(self),
            streams.CreativesStream(self),
            streams.VideoAdsStream(self),
        ]


if __name__ == "__main__":
    TapLinkedInAds.cli()
