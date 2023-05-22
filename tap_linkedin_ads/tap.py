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
    """LinkedInAds tap class."""

    name = "tap-linkedin-ads"

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
            default=NOW.isoformat(),
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
        return [
            streams.Accounts(self),
            streams.VideoAds(self),
            streams.AccountUsers(self),
            # streams.Creatives(self),  # noqa: ERA001
            streams.Campaigns(self),
            streams.CampaignGroups(self),
            streams.AdAnalyticsByCampaign(self),
            streams.AdAnalyticsByCreative(self),
        ]


if __name__ == "__main__":
    TapLinkedInAds.cli()
