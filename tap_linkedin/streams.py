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

    replication_keys = ["end_at"]
    schema_filepath = SCHEMAS_DIR / "accounts.json"
    tap_stream_id = "accounts"
    replication_method = "INCREMENTAL"
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
        #"count": 10000
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

class AccountUsers(LinkedInStream):
    """
    https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-account-users#find-ad-account-users-by-accounts
    """
    tap_stream_id = "account_users"
    replication_keys = ["last_modified_time"]
    replication_method = "INCREMENTAL"
    key_properties = ["account_id", "user_person_id"]
    account_filter = "accounts_param"
    path = "adAccountUsers"
    data_key = "elements"
    params = {
        "q": "accounts"
    }

class CampaignGroups(LinkedInStream):
    """
    https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-campaign-groups#search-for-campaign-groups
    """
    tap_stream_id = "campaign_groups"
    replication_method = "INCREMENTAL"
    replication_keys = ["last_modified_time"]
    key_properties = ["id"]
    account_filter = "search_account_values_param"
    path = "adCampaignGroups"
    data_key = "elements"
    params = {
        "q": "search",
        "sort.field": "ID",
        "sort.order": "ASCENDING"
    }

class Campaigns(LinkedInStream):
    """
    https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-campaigns#search-for-campaigns
    """
    tap_stream_id = "campaigns"
    replication_method = "INCREMENTAL"
    replication_keys = ["last_modified_time"]
    key_properties = ["id"]
    account_filter = "search_account_values_param"
    path = "adCampaigns"
    data_key = "elements"
    children = ["ad_analytics_by_campaign", "creatives", "ad_analytics_by_creative"]
    params = {
        "q": "search",
        "sort.field": "ID",
        "sort.order": "ASCENDING"
    }

class Creatives(LinkedInStream):
    """
    https://learn.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-creatives?view=li-lms-2023-01&tabs=http#search-for-creatives
    """
    tap_stream_id = "creatives"
    replication_method = "INCREMENTAL"
    replication_keys = ["last_modified_at"]
    key_properties = ["id"]
    path = "creatives"
    foreign_key = "id"
    data_key = "elements"
    parent = "campaigns"
    # The value of the campaigns in the query params should be passed in the encoded format.
    # Ref - https://learn.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-creatives?view=li-lms-2023-01&tabs=http#sample-request-3
    params = {
        "q": "criteria",
        "campaigns": "List(urn%3Ali%3AsponsoredCampaign%3A{})",
        "sortOrder": "ASCENDING"
    }
    # Requires this specific headers for creatives endpoint.
    # Ref - https://learn.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-creatives?view=li-lms-2023-01&tabs=http#search-for-creatives
    headers = {'X-Restli-Protocol-Version': "2.0.0",
               "X-RestLi-Method": "FINDER"}


class AdAnalyticsByCreative(LinkedInStream):
    """
    https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting#analytics-finder
    """
    tap_stream_id = "ad_analytics_by_creative"
    replication_method = "INCREMENTAL"
    replication_keys = ["end_at"]
    key_properties = ["creative_id", "start_at"]
    account_filter = "accounts_param"
    path = "adAnalytics"
    foreign_key = "id"
    data_key = "elements"
    parent = "campaigns"
    params = {
        "q": "analytics",
        "pivot": "CREATIVE",
        "timeGranularity": "DAILY",
        "count": 10000
    }