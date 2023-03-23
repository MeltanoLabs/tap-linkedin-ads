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
    # TODO : Adding Replication support
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

class AdAnalyticsByCampaign(LinkedInStream):
    """
    https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting#analytics-finder
    """
    # TODO : Adding Replication support
    name = "ad_analytics_by_campaign"
    #replication_method = "INCREMENTAL"
    schema_filepath = SCHEMAS_DIR / "ad_analytics_by_campaign.json"
    replication_keys = ["end_at"]
    key_properties = ["campaign_id", "start_at"]
    account_filter = "accounts_param"
    path = "adAnalytics"
    foreign_key = "id"
    data_key = "elements"
    parent = "campaigns"

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

class AccountUsers(LinkedInStream):
    """
    https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-account-users#find-ad-account-users-by-accounts
    """
    # TODO : Adding Replication support
    name = "account_users"
    #replication_keys = ["last_modified_time"]
    #replication_method = "INCREMENTAL"
    key_properties = ["account_id", "user_person_id"]
    account_filter = "accounts_param"
    schema_filepath = SCHEMAS_DIR / "account_users.json"
    path = "adAccountUsers"
    data_key = "elements"

class CampaignGroups(LinkedInStream):
    """
    https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-campaign-groups#search-for-campaign-groups
    """
    # TODO : Adding Replication support
    name = "campaign_groups"
    #replication_method = "INCREMENTAL"
    replication_keys = ["last_modified_time"]
    key_properties = ["id"]
    schema_filepath = SCHEMAS_DIR / "campaign_groups.json"
    account_filter = "search_account_values_param"
    path = "adCampaignGroups"
    data_key = "elements"

class Campaigns(LinkedInStream):
    """
    https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-campaigns#search-for-campaigns
    """
    # TODO : Adding Replication support
    name = "campaigns"
    #replication_method = "INCREMENTAL"
    replication_keys = ["last_modified_time"]
    key_properties = ["id"]
    account_filter = "search_account_values_param"
    path = "adCampaigns"
    schema_filepath = SCHEMAS_DIR / "campaigns.json"
    data_key = "elements"
    children = ["ad_analytics_by_campaign", "creatives", "ad_analytics_by_creative"]

class Creatives(LinkedInStream):
    """
    https://learn.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-creatives?view=li-lms-2023-01&tabs=http#search-for-creatives
    """
    name = "creatives"
    replication_method = "INCREMENTAL"
    replication_keys = ["last_modified_at"]
    key_properties = ["id"]
    path = "creatives"
    schema_filepath = SCHEMAS_DIR / "creatives.json"
    foreign_key = "id"
    data_key = "elements"
    parent = "campaigns"



class AdAnalyticsByCreative(LinkedInStream):
    """
    https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting#analytics-finder
    """
    # TODO : Adding Replication support
    name = "ad_analytics_by_creative"
    #replication_method = "INCREMENTAL"
    replication_keys = ["end_at"]
    key_properties = ["creative_id", "start_at"]
    account_filter = "accounts_param"
    schema_filepath = SCHEMAS_DIR / "ad_analytics_by_creative.json"
    path = "adAnalytics"
    foreign_key = "id"
    data_key = "elements"
    parent = "campaigns"
