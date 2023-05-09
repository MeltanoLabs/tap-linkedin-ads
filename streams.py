"""Stream type classes for tap-linkedin-sdk."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pendulum
from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_linkedin.client import LinkedInStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

PropertiesList = th.PropertiesList
Property = th.Property
ObjectType = th.ObjectType
DateTimeType = th.DateTimeType
StringType = th.StringType
ArrayType = th.ArrayType
BooleanType = th.BooleanType
IntegerType = th.IntegerType


class Accounts(LinkedInStream):
    """Accounts stream.

    https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-accounts#search-for-accounts

    columns: columns which will be added to fields parameter in api
    name: stream name
    path: path which will be added to api url in client.py
    schema: instream schema
    primary_keys = primary keys for the table
    replication_keys = datetime keys for replication
    """

    columns = [
        "CHANGE_AUDIT_STAMPS",
        "CREATED_TIME",
        "CURRENCY",
        "ID",
        "LAST_MODIFIED_TIME",
        "NAME",
        "NOTIFIED_ON_CAMPAIGN_OPTIMIZATION",
        "NOTIFIED_ON_CREATIVE_APPROVAL",
        "NOTIFIED_ON_CREATIVE_REJECTION",
        "NOTIFIED_ON_END_OF_CAMPAIGN",
        "NOTIFIED_ON_NEW_FEATURES_ENABLED",
        "REFERENCE",
        "REFERENCE_ORGANIZATION_ID",
        "REFERENCE_PERSON_ID",
        "SERVING_STATUSES",
        "STATUS",
        "TEST",
        "TOTAL_BUDGET",
        "TOTAL_BUDGET_ENDS_AT",
        "TYPE",
        "VERSION",
    ]

    name = "account"
    replication_keys = ["last_modified_time"]
    primary_keys = ["last_modified_time", "id"]
    replication_method = "incremental"
    path = "adAccounts"

    schema = PropertiesList(
        Property(
            "changeAuditStamps",
            ObjectType(
                Property(
                    "created",
                    ObjectType(
                        Property("time", StringType),
                        additional_properties=False,
                    ),
                ),
                Property(
                    "lastModified",
                    ObjectType(
                        Property("time", StringType),
                        additional_properties=False,
                    ),
                ),
            ),
        ),
        Property("created_time", StringType),
        Property("last_modified_time", StringType),
        Property("currency", StringType),
        Property("id", IntegerType),
        Property("name", StringType),
        Property("notifiedOnCampaignOptimization", BooleanType),
        Property("notifiedOnCreativeApproval", BooleanType),
        Property("notifiedOnCreativeRejection", BooleanType),
        Property("notifiedOnEndOfCampaign", BooleanType),
        Property("notifiedOnNewFeaturesEnabled", BooleanType),
        Property("reference", StringType),
        Property("reference_organization_id", IntegerType),
        Property("reference_person_id", StringType),
        Property(
            "servingStatuses",
            th.ArrayType(
                Property("items", StringType),
            ),
        ),
        Property("status", StringType),
        Property(
            "total_budget",
            ObjectType(
                Property("amount", StringType),
                Property("currency_code", StringType),
                additional_properties=False,
            ),
        ),
        Property("total_budget_ends_at", StringType),
        Property("type", StringType),
        Property("test", BooleanType),
        Property(
            "version",
            ObjectType(Property("versionTag", StringType), additional_properties=False),
        ),
    ).to_dict()

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}
        if next_page_token:
            params["start"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key

        params["q"] = "search"
        params["sort.field"] = "ID"
        params["sort.order"] = "ASCENDING"

        return params


class AdAnalyticsByCampaign(LinkedInStream):
    """Ad analytics by campaign stream.

    https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting#analytics-finder

    columns: columns which will be added to fields parameter in api
    name: stream name
    path: path which will be added to api url in client.py
    schema: instream schema
    primary_keys = primary keys for the table
    replication_keys = datetime keys for replication
    """

    name = "ad_analytics_by_campaign"
    replication_keys = ["end_at"]
    replication_method = "incremental"
    key_properties = ["campaign_id", "start_at"]
    path = "adAnalytics"

    ## TODO: CHANGE ALL COLUMN NAMES FROM SNAKE CASE INTO CAMEL CASE
    schema = PropertiesList(
        Property("costInUsd", StringType),
        Property("viralCardClicks", IntegerType),
        Property("adUnitClicks", IntegerType),
        Property("comments", IntegerType),
        Property(
            "dateRange",
            ObjectType(
                Property(
                    "end",
                    ObjectType(
                        Property("day", IntegerType),
                        Property("month", IntegerType),
                        Property("year", IntegerType),
                        additional_properties=False,
                    ),
                ),
                Property(
                    "start",
                    ObjectType(
                        Property("day", IntegerType),
                        Property("month", IntegerType),
                        Property("year", IntegerType),
                        additional_properties=False,
                    ),
                ),
            ),
        ),
        Property("externalWebsitePostViewConversions", IntegerType),
        Property("impressions", IntegerType),
        Property("landingPageClicks", IntegerType),
        Property("oneClickLeadFormOpens", IntegerType),
        Property("pivotValue", StringType),
        Property("sends", IntegerType),
        Property("shares", IntegerType),
        Property("videoCompletions", IntegerType),
        Property("videoMidpointCompletions", IntegerType),
        Property("viralCompanyPageClicks", IntegerType),
        Property("viralExternalWebsitePostViewConversions", IntegerType),
        Property("viralFullScreenPlays", IntegerType),
        Property("viralShares", IntegerType),
        Property("viralTotalEngagements", IntegerType),
        Property("viralVideoStarts", IntegerType),
    ).to_dict()

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        columns = "actionClicks, viralShares, pivotValue, videoCompletions, viralVideoStarts, comments, externalWebsitePostViewConversions, costInUsd, dateRange, landingPageClicks, oneClickLeadFormOpens, impressions, sends, shares, viralFullScreenPlays, videoMidpointCompletions, viralCardClicks, viralTotalEngagements, viralExternalWebsitePostViewConversions, viralCompanyPageClicks"

        params: dict = {}
        if next_page_token:
            params["start"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key

        start_date = pendulum.parse(self.config.get("start_date"))
        end_date = pendulum.parse(self.config.get("end_date"))

        params["q"] = "analytics"
        params["pivot"] = "CAMPAIGN"
        params["timeGranularity"] = "DAILY"
        params["dateRange.start.day"] = start_date.day
        params["dateRange.start.month"] = start_date.month
        params["dateRange.start.year"] = start_date.year
        params["dateRange.end.day"] = end_date.day
        params["dateRange.end.month"] = end_date.month
        params["dateRange.end.year"] = end_date.year
        params["fields"] = columns
        params["campaigns[0]"] = "urn:li:sponsoredCampaign:" + self.config.get(
            "campaign",
        )

        return params


class VideoAds(LinkedInStream):
    """Video ads stream.

    https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/advertising-targeting/create-and-manage-video#finders

    columns: columns which will be added to fields parameter in api
    name: stream name
    path: path which will be added to api url in client.py
    schema: instream schema
    primary_keys = primary keys for the table
    replication_keys = datetime keys for replication
    """

    name = "video_ads"
    replication_keys = ["last_modified_time"]
    replication_method = "incremental"
    primary_keys = ["last_modified_time"]
    path = "adDirectSponsoredContents"

    schema = PropertiesList(
        Property("account", StringType),
        Property("account_id", IntegerType),
        Property(
            "changeAuditStamps",
            ObjectType(
                Property(
                    "created",
                    ObjectType(
                        Property("time", StringType),
                        additional_properties=False,
                    ),
                ),
                Property(
                    "lastModified",
                    ObjectType(
                        Property("time", StringType),
                        additional_properties=False,
                    ),
                ),
            ),
        ),
        Property("created_time", StringType),
        Property("last_modified_time", StringType),
        Property("content_reference", StringType),
        Property("content_reference_ucg_post_id", IntegerType),
        Property("content_reference_share_id", IntegerType),
        Property("name", StringType),
        Property("type", StringType),
    ).to_dict()

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}
        if next_page_token:
            params["start"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key

        params["q"] = "account"
        params["account"] = "urn:li:sponsoredAccount:" + self.config.get("account_id")
        params["owner"] = "urn:li:organization:" + self.config.get("owner")

        return params


class AccountUsers(LinkedInStream):
    """Account users stream.

    https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-account-users#find-ad-account-users-by-accounts

    columns: columns which will be added to fields parameter in api
    name: stream name
    path: path which will be added to api url in client.py
    schema: instream schema
    primary_keys = primary keys for the table
    replication_keys = datetime keys for replication
    """

    columns = [
        "ACCOUNT",
        "ACCOUNT_ID",
        "CAMPAIGN_CONTACT",
        "CHANGE_AUDIT_STAMPS",
        "CREATED_TIME",
        "LAST_MODIFIED_TIME",
        "ROLE",
        "USER",
        "USER_PERSON_ID",
    ]

    name = "account_user"
    replication_keys = ["last_modified_time"]
    replication_method = "incremental"
    primary_keys = ["last_modified_time"]
    path = "adAccountUsers"

    schema = PropertiesList(
        Property("account", StringType),
        Property("campaign_contact", BooleanType),
        Property("account_id", IntegerType),
        Property(
            "changeAuditStamps",
            ObjectType(
                Property(
                    "created",
                    ObjectType(
                        Property("time", StringType),
                        additional_properties=False,
                    ),
                ),
                Property(
                    "lastModified",
                    ObjectType(
                        Property("time", StringType),
                        additional_properties=False,
                    ),
                ),
            ),
        ),
        Property("created_time", StringType),
        Property("last_modified_time", StringType),
        Property("role", StringType),
        Property("user", StringType),
        Property("user_person_id", StringType),
    ).to_dict()

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}
        if next_page_token:
            params["start"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key

        params["q"] = "accounts"
        params["accounts"] = "urn:li:sponsoredAccount:" + self.config.get("account_id")

        return params


class CampaignGroups(LinkedInStream):
    """
    https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-campaign-groups#search-for-campaign-groups
    """

    """
    columns: columns which will be added to fields parameter in api
    name: stream name
    path: path which will be added to api url in client.py
    schema: instream schema
    primary_keys = primary keys for the table
    replication_keys = datetime keys for replication
    """

    name = "campaign_groups"
    replication_keys = ["last_modified_time"]
    replication_method = "incremental"
    primary_keys = ["last_modified_time", "id"]
    path = "adCampaignGroups"

    PropertiesList = th.PropertiesList
    Property = th.Property
    ObjectType = th.ObjectType
    DateTimeType = th.DateTimeType
    StringType = th.StringType
    ArrayType = th.ArrayType
    BooleanType = th.BooleanType
    IntegerType = th.IntegerType

    jsonschema = PropertiesList(
        Property(
            "runSchedule",
            ObjectType(Property("start", DateTimeType), Property("end", DateTimeType)),
        ),
        Property(
            "changeAuditStamps",
            ObjectType(
                Property(
                    "created",
                    ObjectType(
                        Property("time", StringType),
                        additional_properties=False,
                    ),
                ),
                Property(
                    "lastModified",
                    ObjectType(
                        Property("time", StringType),
                        additional_properties=False,
                    ),
                ),
            ),
        ),
        Property("created_time", DateTimeType),
        Property("last_modified_time", DateTimeType),
        Property("name", StringType),
        Property("servingStatuses", ArrayType(StringType)),
        Property("backfilled", BooleanType),
        Property("id", IntegerType),
        Property("account", StringType),
        Property("account_id", IntegerType),
        Property("status", StringType),
        Property(
            "total_budget",
            ObjectType(
                Property("currency_code", StringType),
                Property("amount", StringType),
            ),
        ),
        Property("test", BooleanType),
        Property("allowed_campaign_types", ArrayType(StringType)),
        Property("run_schedule_start", DateTimeType),
        Property("run_schedule_end", StringType),
    ).to_dict()

    schema = jsonschema

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}
        if next_page_token:
            params["start"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key

        params["q"] = "search"
        params["sort.field"] = "ID"
        params["sort.order"] = "ASCENDING"

        return params


class Campaigns(LinkedInStream):
    """Campaigns stream.

    https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-campaigns#search-for-campaigns

    columns: columns which will be added to fields parameter in api
    name: stream name
    path: path which will be added to api url in client.py
    schema: instream schema
    primary_keys = primary keys for the table
    replication_keys = datetime keys for replication
    """

    name = "campaign"
    replication_keys = ["last_modified_time"]
    replication_method = "incremental"
    primary_keys = ["last_modified_time", "id"]
    path = "adCampaigns"

    schema = PropertiesList(
        Property(
            "targeting",
            ObjectType(
                Property(
                    "created",
                    ObjectType(
                        Property(
                            "included_targeting_facets",
                            th.ArrayType(
                                Property(
                                    "items",
                                    ObjectType(
                                        Property("type", StringType),
                                        Property(
                                            "values",
                                            th.ArrayType(Property("items", StringType)),
                                        ),
                                        additional_properties=False,
                                    ),
                                ),
                            ),
                        ),
                        Property(
                            "excluded_targeting_facets",
                            th.ArrayType(
                                Property(
                                    "items",
                                    ObjectType(
                                        Property("type", StringType),
                                        Property(
                                            "values",
                                            th.ArrayType(Property("items", StringType)),
                                        ),
                                        additional_properties=False,
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        ),
        Property(
            "targetingCriteria",
            ObjectType(
                Property(
                    "include",
                    ObjectType(
                        Property(
                            "and",
                            th.ArrayType(
                                Property(
                                    "items",
                                    ObjectType(
                                        Property("type", StringType),
                                        Property(
                                            "values",
                                            th.ArrayType(Property("items", StringType)),
                                        ),
                                        additional_properties=False,
                                    ),
                                ),
                            ),
                        ),
                        Property(
                            "or",
                            th.ArrayType(
                                Property(
                                    "items",
                                    ObjectType(
                                        Property("type", StringType),
                                        Property(
                                            "values",
                                            th.ArrayType(Property("items", StringType)),
                                        ),
                                        additional_properties=False,
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                Property(
                    "exclude",
                    ObjectType(
                        Property(
                            "or",
                            ObjectType(
                                Property(
                                    "urn:li:ad_targeting_facet:titles",
                                    th.ArrayType(
                                        Property("items", StringType),
                                    ),
                                ),
                                Property(
                                    "urn:li:ad_targeting_facet:staff_count_ranges",
                                    th.ArrayType(
                                        Property("items", StringType),
                                    ),
                                ),
                                Property(
                                    "urn:li:ad_targeting_facet:followed_companies",
                                    th.ArrayType(
                                        Property("items", StringType),
                                    ),
                                ),
                                Property(
                                    "urn:li:ad_targeting_facet:seniorities",
                                    th.ArrayType(
                                        Property("items", StringType),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        ),
        Property("servingStatuses", th.ArrayType(Property("items", StringType))),
        Property(
            "totalBudget",
            ObjectType(
                Property("amount", StringType),
                Property("currencyCode", StringType),
                additional_properties=False,
            ),
        ),
        Property("version_tag", StringType),
        Property(
            "locale",
            ObjectType(
                Property("country", StringType),
                Property("language", StringType),
                additional_properties=False,
            ),
        ),
        Property(
            "version",
            ObjectType(Property("versionTag", StringType), additional_properties=False),
        ),
        Property("associatedEntity", StringType),
        Property("associated_entity_organization_id", IntegerType),
        Property("associated_entity_person_id", IntegerType),
        Property(
            "runSchedule",
            ObjectType(
                Property("start", StringType),
                Property("end", StringType),
                additional_properties=False,
            ),
        ),
        Property("optimizationTargetType", StringType),
        Property(
            "changeAuditStamps",
            ObjectType(
                Property(
                    "created",
                    ObjectType(
                        Property("time", StringType),
                        additional_properties=False,
                    ),
                ),
                Property(
                    "lastModified",
                    ObjectType(
                        Property("time", StringType),
                        additional_properties=False,
                    ),
                ),
            ),
        ),
        Property("campaignGroup", StringType),
        Property("campaign_group_id", IntegerType),
        Property(
            "dailyBudget",
            ObjectType(
                Property("amount", StringType),
                Property("currencyCode", StringType),
                additional_properties=False,
            ),
        ),
        Property(
            "unitCost",
            ObjectType(
                Property("amount", StringType),
                Property("currencyCode", StringType),
                additional_properties=False,
            ),
        ),
        Property("creativeSelection", StringType),
        Property("costType", StringType),
        Property("name", StringType),
        Property("objectiveType", StringType),
        Property("offsiteDeliveryEnabled", BooleanType),
        Property(
            "offsitePreferences",
            ObjectType(
                Property(
                    "iabCategories",
                    ObjectType(
                        Property(
                            "exclude",
                            th.ArrayType(
                                Property("items", StringType),
                            ),
                        ),
                        Property(
                            "include",
                            th.ArrayType(Property("items", StringType)),
                        ),
                    ),
                ),
                Property(
                    "publisherRestrictionFiles",
                    ObjectType(
                        Property(
                            "exclude",
                            th.ArrayType(Property("items", StringType)),
                        ),
                    ),
                ),
            ),
        ),
        Property("id", IntegerType),
        Property("audienceExpansionEnabled", BooleanType),
        Property("test", BooleanType),
        Property("format", StringType),
        Property("pacingStrategy", StringType),
        Property("account", StringType),
        Property("account_id", IntegerType),
        Property("status", StringType),
        Property("type", StringType),
        Property("storyDeliveryEnabled", BooleanType),
        Property("created_time", DateTimeType),
        Property("last_modified_time", DateTimeType),
        Property("run_schedule_start", DateTimeType),
        Property("run_schedule_end", StringType),
    ).to_dict()

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}
        if next_page_token:
            params["start"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key

        params["q"] = "search"
        params["sort.field"] = "ID"
        params["sort.order"] = "ASCENDING"

        return params


class Creatives(LinkedInStream):
    """Creatives stream.

    https://learn.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-creatives?view=li-lms-2023-01&tabs=http#search-for-creatives

    columns: columns which will be added to fields parameter in api
    name: stream name
    path: path which will be added to api url in client.py
    schema: instream schema
    primary_keys = primary keys for the table
    replication_keys = datetime keys for replication
    """

    name = "creatives"
    replication_keys = ["last_modified_time"]
    replication_method = "incremental"
    primary_keys = ["last_modified_time", "id"]
    path = "creatives"

    schema = PropertiesList(
        Property("account", StringType),
        Property("account_id", IntegerType),
        Property("campaign", StringType),
        Property("campaign_id", IntegerType),
        Property(
            "content",
            ObjectType(
                Property("reference", StringType),
                Property(
                    "text_ad",
                    ObjectType(
                        Property("headline", StringType),
                        Property("description", StringType),
                        Property("landing_page", StringType),
                        additional_properties=False,
                    ),
                ),
            ),
        ),
        Property("created_at", StringType),
        Property("created_by", StringType),
        Property("last_modified_at", StringType),
        Property("last_modified_by", StringType),
        Property("id", StringType),
        Property("intended_status", StringType),
        Property("is_serving", BooleanType),
        Property("is_test", BooleanType),
        Property("serving_hold_reasons", th.ArrayType(Property("items", StringType))),
    ).to_dict()

    def http_headers(self) -> dict:
        """Return the HTTP headers needed for this endpoint."""
        headers = super().http_headers()

        # Override the protocol version to 2.0.0
        headers["X-Restli-Protocol-Version"] = "2.0.0"
        return headers

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}
        if next_page_token:
            params["start"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key

        ## TODO: Resolve issue with parantheses in campaigns parameter being encoded by rest.py
        params["campaigns"] = "urn:li:sponsoredCampaign:" + self.config.get("campaign")
        params["q"] = "criteria"

        return params


class AdAnalyticsByCreative(LinkedInStream):
    """Ad Analytics by Creative stream.

    https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting#analytics-finder

    columns: columns which will be added to fields parameter in api
    name: stream name
    path: path which will be added to api url in client.py
    schema: instream schema
    primary_keys = primary keys for the table
    replication_keys = datetime keys for replication
    """

    name = "ad_analytics_by_creative"
    replication_keys = ["end_at"]
    replication_method = "incremental"
    key_properties = ["creative_id", "start_at"]
    path = "adAnalytics"

    ## TODO: CHANGE ALL COLUMN NAMES FROM SNAKE CASE INTO CAMEL CASE
    schema = PropertiesList(
        Property("costInUsd", StringType),
        Property("viralCardClicks", IntegerType),
        Property("adUnitClicks", IntegerType),
        Property("comments", IntegerType),
        Property(
            "dateRange",
            ObjectType(
                Property(
                    "end",
                    ObjectType(
                        Property("day", IntegerType),
                        Property("month", IntegerType),
                        Property("year", IntegerType),
                        additional_properties=False,
                    ),
                ),
                Property(
                    "start",
                    ObjectType(
                        Property("day", IntegerType),
                        Property("month", IntegerType),
                        Property("year", IntegerType),
                        additional_properties=False,
                    ),
                ),
            ),
        ),
        Property("externalWebsitePostViewConversions", IntegerType),
        Property("impressions", IntegerType),
        Property("landingPageClicks", IntegerType),
        Property("oneClickLeadFormOpens", IntegerType),
        Property("pivotValue", StringType),
        Property("sends", IntegerType),
        Property("shares", IntegerType),
        Property("videoCompletions", IntegerType),
        Property("videoMidpointCompletions", IntegerType),
        Property("viralCompanyPageClicks", IntegerType),
        Property("viralExternalWebsitePostViewConversions", IntegerType),
        Property("viralFullScreenPlays", IntegerType),
        Property("viralShares", IntegerType),
        Property("viralTotalEngagements", IntegerType),
        Property("viralVideoStarts", IntegerType),
    ).to_dict()

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        columns = "actionClicks, viralShares, pivotValue, videoCompletions, viralVideoStarts, comments, externalWebsitePostViewConversions, costInUsd, dateRange, landingPageClicks, oneClickLeadFormOpens, impressions, sends, shares, viralFullScreenPlays, videoMidpointCompletions, viralCardClicks, viralTotalEngagements, viralExternalWebsitePostViewConversions, viralCompanyPageClicks"

        params: dict = {}
        if next_page_token:
            params["start"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key

        params["fields"] = columns

        start_date = pendulum.parse(self.config.get("start_date"))
        end_date = pendulum.parse(self.config.get("end_date"))

        params["q"] = "analytics"
        params["pivot"] = "CREATIVE"
        params["timeGranularity"] = "DAILY"
        params["dateRange.start.day"] = start_date.day
        params["dateRange.start.month"] = start_date.month
        params["dateRange.start.year"] = start_date.year
        params["dateRange.end.day"] = end_date.day
        params["dateRange.end.month"] = end_date.month
        params["dateRange.end.year"] = end_date.year
        params["campaigns[0]"] = "urn:li:sponsoredCampaign:" + self.config.get(
            "campaign",
        )

        return params
