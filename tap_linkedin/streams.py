"""Stream type classes for tap-linkedin-sdk."""

from __future__ import annotations

from pathlib import Path

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_linkedin.client import LinkedInStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class Accounts(LinkedInStream):
    """
    https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-accounts#search-for-accounts
    """
    name = "accounts"
    path = "adAccounts"
    primary_keys = ["id"]

    replication_keys = ["last_modified_time"]
    schema_filepath = SCHEMAS_DIR / "accounts.json"
    tap_stream_id = "accounts"
    #replication_method = "INCREMENTAL"
    account_filter = "search_id_values_param"
    data_key = "elements"
    children = ["video_ads"]
    params = {
        "q": "search"
    }

class AdAnalyticsByCampaign(LinkedInStream):
    """
    https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting#analytics-finder
    """
    name = "ad_analytics_by_campaign"
    replication_method = "INCREMENTAL"
    schema_filepath = SCHEMAS_DIR / "ad_analytics_by_campaign.json"
    replication_keys = ["end_at"]
    key_properties = ["campaign_id", "start_at"]
    account_filter = "accounts_param"
    path = "adAnalytics"
    foreign_key = "id"
    data_key = "elements"
    parent = "campaigns"
    params = {
        "q": "analytics",
        "pivot": "CAMPAIGN",
        "timeGranularity": "DAILY",
        "count": 10000
    }

class VideoAds(LinkedInStream):
    """
    https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/advertising-targeting/create-and-manage-video#finders
    """
    name = "video_ads"
    path = "adDirectSponsoredContents"
    replication_keys = ["last_modified_time"]
    replication_method = "INCREMENTAL"
    key_properties = ["content_reference"]
    schema_filepath = SCHEMAS_DIR / "video_ads.json"
    foreign_key = "id"
    data_key = "elements"
    parent = "accounts"
    params = {
        "q": "account"
    }