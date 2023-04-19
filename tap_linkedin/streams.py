"""Stream type classes for tap-linkedin-sdk."""

from __future__ import annotations

from pathlib import Path

import singer_sdk

from singer_sdk import typing as th  # JSON Schema typing helpers

PropertiesList = th.PropertiesList
Property = th.Property
ObjectType = th.ObjectType
DateTimeType = th.DateTimeType
StringType = th.StringType
ArrayType = th.ArrayType
BooleanType = th.BooleanType
IntegerType = th.IntegerType


from tap_linkedin.client import LinkedInStream

import pendulum

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class Accounts(LinkedInStream):
    """
    https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-accounts#search-for-accounts
    """

    """
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
    primary_keys = ["id"]
    replication_keys = ["last_modified_time"]
    replication_method = "incremental"
    path = "adAccounts"

    schema = PropertiesList(

        Property(
            "changeAuditStamps",
            ObjectType(
                Property("created",
                    ObjectType(
                        Property("time", StringType),
                        additional_properties=False
                    ),
                ),
                Property("lastModified",
                    ObjectType(
                        Property("time", StringType),
                        additional_properties=False
                    ),
                ),
            )
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

        Property("servingStatuses",
                 th.ArrayType(
                     Property("items", StringType)
                 )
        ),

        Property("status", StringType),

        Property("total_budget",
                 ObjectType(
                     Property("amount", StringType),
                     Property("currency_code", StringType),
                     additional_properties=False
                 ),
        ),

        Property("total_budget_ends_at", StringType),
        Property("type", StringType),
        Property("test", BooleanType),

        Property("version",
                 ObjectType(
                     Property("versionTag", StringType),
                     additional_properties=False
                 ),
        )

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
    """
    https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting#analytics-finder
    """

    """
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

    schema = PropertiesList(

        Property(
            "average_daily_reach_metrics",
            ObjectType(
                Property("approximate_cost_in_currency_per_thousand_members_reached", StringType),
                Property("approximate_reach", StringType),
                Property("approximate_frequency", StringType),
                additional_properties=False
            )
        ),

        Property(
            "average_previous_seven_day_reach_metrics",
            ObjectType(
                Property("approximate_cost_in_currency_per_thousand_members_reached", StringType),
                Property("approximate_reach", StringType),
                Property("approximate_frequency", StringType),
                additional_properties=False
            )
        ),

        Property(
            "average_previous_thirty_day_reach_metrics",
            ObjectType(
                Property("approximate_cost_in_currency_per_thousand_members_reached", StringType),
                Property("approximate_reach", StringType),
                Property("approximate_frequency", StringType),
                additional_properties=False
            )
        ),

        Property("document_completions", IntegerType),
        Property("document_first_quartile_completions", IntegerType),
        Property("clicks", IntegerType),
        Property("document_midpoint_completions", IntegerType),
        Property("document_third_quartile_completions", IntegerType),
        Property("download_clicks", IntegerType),
        Property("job_applications", StringType),
        Property("job_apply_clicks", StringType),
        Property("post_click_job_applications", StringType),
        Property("post_click_job_apply_clicks", StringType),
        Property("post_click_registrations", StringType),
        Property("post_view_job_applications", StringType),
        Property("post_view_job_apply_clicks", StringType),
        Property("cost_in_usd", StringType),
        Property("post_view_registrations", StringType),
        Property("registrations", StringType),
        Property("talent_leads", IntegerType),
        Property("viral_document_completions", IntegerType),
        Property("viral_document_first_quartile_completions", IntegerType),
        Property("viral_document_midpoint_completions", IntegerType),
        Property("viral_document_third_quartile_completions", IntegerType),
        Property("viral_download_clicks", IntegerType),
        Property("viral_job_applications", StringType),
        Property("viral_job_apply_clicks", StringType),
        Property("cost_in_local_currency", StringType),
        Property("viral_post_click_job_applications", StringType),
        Property("viral_post_click_job_apply_clicks", StringType),
        Property("viral_post_click_registrations", StringType),
        Property("viral_post_view_job_applications", StringType),
        Property("viral_post_view_job_apply_clicks", StringType),
        Property("viral_post_view_registrations", StringType),
        Property("viral_registrations", StringType),
        Property("approximate_unique_impressions", IntegerType),
        Property("card_clicks", IntegerType),
        Property("card_impressions", IntegerType),
        Property("comment_likes", IntegerType),
        Property("viral_card_clicks", IntegerType),
        Property("viral_card_impressions", IntegerType),
        Property("viral_comment_likes", IntegerType),
        Property("campaign", StringType),
        Property("campaign_id", IntegerType),
        Property("start_at", StringType),
        Property("end_at", StringType),
        Property("action_clicks", IntegerType),
        Property("ad_unit_clicks", IntegerType),
        Property("comments", IntegerType),
        Property("company_page_clicks", IntegerType),
        Property("conversion_value_in_local_currency", StringType),

        Property(
            "date_range",
            ObjectType(
                Property("end",
                    ObjectType(
                        Property("day", IntegerType),
                        Property("month", IntegerType),
                        Property("year", IntegerType),
                        additional_properties=False
                    )
                ),
                Property("start",
                    ObjectType(
                        Property("day", IntegerType),
                        Property("month", IntegerType),
                        Property("year", IntegerType),
                        additional_properties=False
                    )
                ),
            )
        ),

        Property("external_website_conversions", IntegerType),
        Property("external_website_post_click_conversions", IntegerType),
        Property("external_website_post_view_conversions", IntegerType),
        Property("follows", IntegerType),
        Property("full_screen_plays", IntegerType),
        Property("impressions", IntegerType),
        Property("landing_page_clicks", IntegerType),
        Property("lead_generation_mail_contact_info_shares", IntegerType),
        Property("lead_generation_mail_interested_clicks", IntegerType),
        Property("likes", IntegerType),
        Property("one_click_lead_form_opens", IntegerType),
        Property("one_click_leads", IntegerType),
        Property("opens", IntegerType),
        Property("other_engagements", IntegerType),
        Property("pivot", StringType),
        Property("pivot_value", StringType),

        Property("pivot_values",
            th.ArrayType(
                Property("items", StringType)
            )
        ),

        Property("reactions", IntegerType),
        Property("sends", IntegerType),
        Property("shares", IntegerType),
        Property("text_url_clicks", IntegerType),
        Property("total_engagements", IntegerType),
        Property("video_completions", IntegerType),
        Property("video_first_quartile_completions", IntegerType),
        Property("video_midpoint_completions", IntegerType),
        Property("video_starts", IntegerType),
        Property("video_third_quartile_completions", IntegerType),
        Property("video_views", IntegerType),
        Property("viral_clicks", IntegerType),
        Property("viral_comments", IntegerType),
        Property("viral_company_page_clicks", IntegerType),
        Property("viral_external_website_conversions", IntegerType),
        Property("viral_external_website_post_click_conversions", IntegerType),
        Property("viral_external_website_post_view_conversions", IntegerType),
        Property("viral_follows", IntegerType),
        Property("viral_full_screen_plays", IntegerType),
        Property("viral_impressions", IntegerType),
        Property("viral_landing_page_clicks", IntegerType),
        Property("viral_likes", IntegerType),
        Property("viral_one_click_lead_form_opens", IntegerType),
        Property("viral_one_click_leads", IntegerType),
        Property("viral_other_engagements", IntegerType),
        Property("viral_reactions", IntegerType),
        Property("viral_shares", IntegerType),
        Property("viral_total_engagements", IntegerType),
        Property("viral_video_completions", IntegerType),
        Property("viral_video_first_quartile_completions", IntegerType),
        Property("viral_video_midpoint_completions", IntegerType),
        Property("viral_video_starts", IntegerType),
        Property("viral_video_third_quartile_completions", IntegerType),
        Property("viral_video_views", IntegerType)

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
        params["campaigns[0]"] = "urn:li:sponsoredCampaign:" + self.config.get("campaign")


        return params


class VideoAds(LinkedInStream):
    """
    https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/advertising-targeting/create-and-manage-video#finders
    """

    """
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
    key_properties = ["content_reference"]
    path = "adDirectSponsoredContents"

    schema = PropertiesList(

        Property("account", StringType),
        Property("account_id", IntegerType),

        Property(
            "changeAuditStamps",
            ObjectType(
                Property("created",
                    ObjectType(
                        Property("time", StringType),
                        additional_properties=False
                    ),
                ),
                Property("lastModified",
                    ObjectType(
                        Property("time", StringType),
                        additional_properties=False
                    ),
                ),
            )
        ),

        Property("created_time", StringType),
        Property("last_modified_time", StringType),
        Property("content_reference", StringType),
        Property("content_reference_ucg_post_id", IntegerType),
        Property("content_reference_share_id", IntegerType),
        Property("name", StringType),
        Property("type", StringType)

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
    """
    https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-account-users#find-ad-account-users-by-accounts
    """

    """
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
    key_properties = ["account_id", "user_person_id"]
    path = "adAccountUsers"

    schema = PropertiesList(

        Property("account", StringType),
        Property("campaign_contact", BooleanType),
        Property("account_id", IntegerType),

        Property(
            "changeAuditStamps",
            ObjectType(
                Property("created",
                         ObjectType(
                             Property("time", StringType),
                             additional_properties=False

                         ),

                ),

                Property("lastModified",
                         ObjectType(
                             Property("time", StringType),
                             additional_properties=False
                         ),

                ),

            )
        ),

        Property("created_time", StringType),
        Property("last_modified_time", StringType),
        Property("role", StringType),
        Property("user", StringType),
        Property("user_person_id", StringType)

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
    key_properties = ["id"]
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
            ObjectType(
                Property("start", DateTimeType),
                Property("end", DateTimeType)
            )
        ),

        Property(
            "changeAuditStamps",
            ObjectType(
                Property("created",
                    ObjectType(
                        Property("time", StringType),
                        additional_properties=False
                    ),
                ),
                Property("lastModified",
                    ObjectType(
                        Property("time", StringType),
                        additional_properties=False
                    ),
                ),
            )
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
                Property("amount", StringType)
            )
        ),

        Property("test", BooleanType),
        Property("allowed_campaign_types", ArrayType(StringType))
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
    """
    https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-campaigns#search-for-campaigns
    """

    """
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
    key_properties = ["id"]
    path = "adCampaigns"

    schema = PropertiesList(

        Property(
            "targeting",
            ObjectType(
                Property("created",
                    ObjectType(
                        Property("included_targeting_facets",
                            th.ArrayType(
                                Property("items",
                                    ObjectType(
                                        Property("type", StringType),
                                        Property("values",
                                            th.ArrayType(
                                                Property("items", StringType)
                                            )
                                        ),
                                        additional_properties=False
                                    )
                                ),
                            )
                        ),
                        Property("excluded_targeting_facets",
                            th.ArrayType(
                                Property("items",
                                    ObjectType(
                                        Property("type", StringType),
                                        Property("values",
                                            th.ArrayType(
                                                Property("items", StringType)
                                            )
                                        ),
                                        additional_properties=False
                                    )
                                ),
                            )
                        ),
                    ),
                ),
            )
        ),

        Property(
            "targetingCriteria",
            ObjectType(
                Property("include",
                    ObjectType(
                        Property("and",
                            th.ArrayType(
                                Property("items",
                                    ObjectType(
                                        Property("type", StringType),
                                        Property("values",
                                            th.ArrayType(
                                                Property("items", StringType)
                                            )
                                        ),
                                        additional_properties=False
                                    )
                                ),
                            )
                        ),
                        Property("or",
                            th.ArrayType(
                                Property("items",
                                    ObjectType(
                                        Property("type", StringType),
                                        Property("values",
                                            th.ArrayType(
                                                Property("items", StringType)
                                            )
                                        ),
                                        additional_properties=False
                                    )
                                ),
                            )
                        ),
                    ),
                ),
                Property("exclude",
                    ObjectType(
                        Property("or",
                            ObjectType(
                                Property("urn:li:ad_targeting_facet:titles",
                                    th.ArrayType(
                                        Property("items", StringType),
                                    )
                                ),
                                Property("urn:li:ad_targeting_facet:staff_count_ranges",
                                    th.ArrayType(
                                        Property("items", StringType),
                                    )
                                ),
                                Property("urn:li:ad_targeting_facet:followed_companies",
                                    th.ArrayType(
                                        Property("items", StringType),
                                    )
                                ),
                                Property("urn:li:ad_targeting_facet:seniorities",
                                    th.ArrayType(
                                        Property("items", StringType),
                                    )
                                ),
                            ),
                        ),
                    ),
                ),
            )
        ),

        Property("servingStatuses",
            th.ArrayType(
                Property("items", StringType)
            )
        ),

        Property("totalBudget",
            ObjectType(
                Property("amount", StringType),
                Property("currencyCode", StringType),
                additional_properties=False
            ),
        ),

        Property("version_tag", StringType),

        Property("locale",
            ObjectType(
                Property("country", StringType),
                Property("language", StringType),
                additional_properties=False
            ),
        ),

        Property("version",
            ObjectType(
                Property("versionTag", StringType),
                additional_properties=False
            ),
        ),

        Property("associatedEntity", StringType),
        Property("associated_entity_organization_id", IntegerType),
        Property("associated_entity_person_id", IntegerType),

        Property("runSchedule",
            ObjectType(
                Property("start", StringType),
                Property("end", StringType),
                additional_properties=False
            ),
        ),

        Property("optimizationTargetType", StringType),

        Property(
            "changeAuditStamps",
            ObjectType(
                Property("created",
                    ObjectType(
                        Property("time", StringType),
                        additional_properties=False
                    ),
                ),
                Property("lastModified",
                    ObjectType(
                        Property("time", StringType),
                        additional_properties=False
                    ),
                ),

            )
        ),

        Property("campaignGroup", StringType),
        Property("campaign_group_id", StringType),

        Property("dailyBudget",
            ObjectType(
                Property("amount", StringType),
                Property("currencyCode", StringType),
                additional_properties=False
            ),
        ),

        Property("unitCost",
            ObjectType(
                Property("amount", StringType),
                Property("currencyCode", StringType),
                additional_properties=False
            ),
        ),

        Property("creativeSelection", StringType),
        Property("costType", StringType),
        Property("name", StringType),
        Property("objectiveType", StringType),
        Property("offsiteDeliveryEnabled", BooleanType),

        Property("offsitePreferences",
            ObjectType(
                Property("iabCategories",
                    ObjectType(
                        Property("exclude",
                            th.ArrayType(
                                Property("items", StringType),
                            )
                        ),
                        Property("include",
                            th.ArrayType(
                                Property("items", StringType)
                            )
                        ),
                    ),
                ),
                Property("publisherRestrictionFiles",
                    ObjectType(
                        Property("exclude",
                            th.ArrayType(
                                Property("items", StringType)
                            )
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
    """
    https://learn.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-creatives?view=li-lms-2023-01&tabs=http#search-for-creatives
    """

    """
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
    key_properties = ["id"]
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
                Property("text_ad",
                    ObjectType(
                        Property("headline", StringType),
                        Property("description", StringType),
                        Property("landing_page", StringType),
                        additional_properties=False
                    ),
                ),
            )
        ),

        Property("created_at", StringType),
        Property("created_by", StringType),
        Property("last_modified_at", StringType),
        Property("last_modified_by", StringType),
        Property("id", StringType),
        Property("intended_status", StringType),
        Property("is_serving", BooleanType),
        Property("is_test", BooleanType),

        Property("serving_hold_reasons",
            th.ArrayType(
                Property("items", StringType)
            )
        )

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

        params["campaigns"] = "urn:li:sponsoredCampaign:" + self.config.get("campaign")
        params["q"] = "criteria"

        return params


class AdAnalyticsByCreative(LinkedInStream):
    """
    https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting#analytics-finder
    """

    """
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

    schema = PropertiesList(

        Property("landing_page_clicks", IntegerType),

        Property(
            "average_daily_reach_metrics",
            ObjectType(
                Property("approximate_cost_in_currency_per_thousand_members_reached", StringType),
                Property("approximate_reach", StringType),
                Property("approximate_frequency", StringType),
                additional_properties=False
            )
        ),

        Property(
            "average_previous_seven_day_reach_metrics",
            ObjectType(
                Property("approximate_cost_in_currency_per_thousand_members_reached", StringType),
                Property("approximate_reach", StringType),
                Property("approximate_frequency", StringType),
                additional_properties=False
            )
        ),

        Property(
            "average_previous_thirty_day_reach_metrics",
            ObjectType(
                Property("approximate_cost_in_currency_per_thousand_members_reached", StringType),
                Property("approximate_reach", StringType),
                Property("approximate_frequency", StringType),
                additional_properties=False
            )
        ),

        Property("document_completions", IntegerType),
        Property("document_first_quartile_completions", IntegerType),
        Property("clicks", IntegerType),
        Property("document_midpoint_completions", IntegerType),
        Property("document_third_quartile_completions", IntegerType),
        Property("download_clicks", IntegerType),
        Property("job_applications", StringType),
        Property("job_apply_clicks", StringType),
        Property("post_click_job_applications", StringType),
        Property("post_click_job_apply_clicks", StringType),
        Property("post_click_registrations", StringType),
        Property("post_view_job_applications", StringType),
        Property("post_view_job_apply_clicks", StringType),
        Property("cost_in_usd", StringType),
        Property("post_view_registrations", StringType),
        Property("registrations", StringType),
        Property("talent_leads", IntegerType),
        Property("viral_document_completions", IntegerType),
        Property("viral_document_first_quartile_completions", IntegerType),
        Property("viral_document_midpoint_completions", IntegerType),
        Property("viral_document_third_quartile_completions", IntegerType),
        Property("viral_download_clicks", IntegerType),
        Property("viral_job_applications", StringType),
        Property("viral_job_apply_clicks", StringType),
        Property("cost_in_local_currency", StringType),
        Property("viral_post_click_job_applications", StringType),
        Property("viral_post_click_job_apply_clicks", StringType),
        Property("viral_post_click_registrations", StringType),
        Property("viral_post_view_job_applications", StringType),
        Property("viral_post_view_job_apply_clicks", StringType),
        Property("viral_post_view_registrations", StringType),
        Property("viral_registrations", IntegerType),
        Property("approximate_unique_impressions", IntegerType),
        Property("card_clicks", IntegerType),
        Property("card_impressions", IntegerType),
        Property("comment_likes", IntegerType),
        Property("viral_card_clicks", IntegerType),
        Property("viral_card_impressions", IntegerType),
        Property("viral_comment_likes", IntegerType),
        Property("creative", StringType),
        Property("creative_id", IntegerType),
        Property("start_at", StringType),
        Property("end_at", StringType),
        Property("action_clicks", IntegerType),
        Property("ad_unit_clicks", IntegerType),
        Property("comments", IntegerType),
        Property("company_page_clicks", IntegerType),
        Property("conversion_value_in_local_currency", StringType),

        Property(
            "date_range",
            ObjectType(
                Property("end",
                    ObjectType(
                        Property("day", IntegerType),
                        Property("month", IntegerType),
                        Property("year", IntegerType),
                        additional_properties=False
                    )
                ),
                Property("start",
                    ObjectType(
                        Property("day", IntegerType),
                        Property("month", IntegerType),
                        Property("year", IntegerType),
                        additional_properties=False
                    )
                ),
            )
        ),

        Property("external_website_conversions", IntegerType),
        Property("external_website_post_click_conversions", IntegerType),
        Property("external_website_post_view_conversions", IntegerType),
        Property("follows", IntegerType),
        Property("full_screen_plays", IntegerType),
        Property("impressions", IntegerType),
        Property("lead_generation_mail_contact_info_shares", IntegerType),
        Property("lead_generation_mail_interested_clicks", IntegerType),
        Property("likes", IntegerType),
        Property("one_click_lead_form_opens", IntegerType),
        Property("one_click_leads", IntegerType),
        Property("opens", IntegerType),
        Property("other_engagements", IntegerType),
        Property("pivot", StringType),
        Property("pivot_value", StringType),

        Property("pivot_values",
            th.ArrayType(
                Property("items", StringType)
            )
        ),

        Property("reactions", IntegerType),
        Property("sends", IntegerType),
        Property("shares", IntegerType),
        Property("text_url_clicks", IntegerType),
        Property("total_engagements", IntegerType),
        Property("video_completions", IntegerType),
        Property("video_first_quartile_completions", IntegerType),
        Property("video_midpoint_completions", IntegerType),
        Property("video_starts", IntegerType),
        Property("video_third_quartile_completions", IntegerType),
        Property("video_views", IntegerType),
        Property("viral_clicks", IntegerType),
        Property("viral_comments", IntegerType),
        Property("viral_company_page_clicks", IntegerType),
        Property("viral_external_website_conversions", IntegerType),
        Property("viral_external_website_post_click_conversions", IntegerType),
        Property("viral_external_website_post_view_conversions", IntegerType),
        Property("viral_follows", IntegerType),
        Property("viral_full_screen_plays", IntegerType),
        Property("viral_impressions", IntegerType),
        Property("viral_landing_page_clicks", IntegerType),
        Property("viral_likes", IntegerType),
        Property("viral_one_click_lead_form_opens", IntegerType),
        Property("viral_one_click_leads", IntegerType),
        Property("viral_other_engagements", IntegerType),
        Property("viral_reactions", IntegerType),
        Property("viral_shares", IntegerType),
        Property("viral_total_engagements", IntegerType),
        Property("viral_video_completions", IntegerType),
        Property("viral_video_first_quartile_completions", IntegerType),
        Property("viral_video_midpoint_completions", IntegerType),
        Property("viral_video_starts", IntegerType),
        Property("viral_video_third_quartile_completions", IntegerType),
        Property("viral_video_views", IntegerType)

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
        params["campaigns[0]"] = "urn:li:sponsoredCampaign:" + self.config.get("campaign")

        return params
