"""Stream type classes for tap-linkedin-ads."""

from __future__ import annotations

import typing as t
from datetime import datetime, timezone
from importlib import resources

from singer_sdk.streams.core import REPLICATION_FULL_TABLE

from tap_linkedin_ads.streams.base_stream import LinkedInAdsStreamBase

SCHEMAS_DIR = resources.files(__package__) / "schemas"
UTC = timezone.utc


class AdAnalyticsBase(LinkedInAdsStreamBase):
    """LinkedInAds stream class for ad analytics."""

    path = "/adAnalytics"
    replication_method = REPLICATION_FULL_TABLE

    substreams: t.ClassVar[list] = []

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        """Post-process each record returned by the API.

        Args:
            row: Individual record in the stream.
            context: Stream partition or context dictionary.

        Returns:
            The resulting record dict, or `None` if the record should be excluded.
        """
        start_date = row.get("dateRange", {}).get("start", {})

        if start_date:
            row["day"] = datetime.strptime(
                f"{start_date.get('year')}-{start_date.get('month')}-{start_date.get('day')}",
                "%Y-%m-%d",
            ).astimezone(UTC)

        return super().post_process(row, context)

    def merge_dicts(self, *dict_args: dict) -> dict:
        """Return a merged dictionary of adAnalytics responses.

        Args:
            *dict_args: dictionaries with adAnalytics response data.

        Returns:
            A merged dictionary of adAnalytics responses
        """
        result = {}
        for dictionary in dict_args:
            result.update(dictionary)
        return result
