"""Stream type classes for tap-linkedin-ads-sdk."""

from __future__ import annotations

import typing as t
from datetime import datetime, timezone
from pathlib import Path

import json
import os
from dotenv import load_dotenv
load_dotenv(".env")

import pendulum
from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_linkedin_ads.client import LinkedInAdsStream

PropertiesList = th.PropertiesList
Property = th.Property
ObjectType = th.ObjectType
DateTimeType = th.DateTimeType
StringType = th.StringType
ArrayType = th.ArrayType
BooleanType = th.BooleanType
IntegerType = th.IntegerType

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
UTC = timezone.utc


class Accounts(LinkedInAdsStream):
    """https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-accounts#search-for-accounts."""

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
    replication_keys = ["last_modified_time"]
    primary_keys = ["last_modified_time", "id", "status"]
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
        Property("servingStatuses", th.ArrayType(Property("items", StringType))),
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
        context: dict | None,  # noqa: ARG002
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
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


class AdAnalyticsByCampaignInit(LinkedInAdsStream):
    """https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting#analytics-finder."""

    """
    columns: columns which will be added to fields parameter in api
    name: stream name
    path: path which will be added to api url in client.py
    schema: instream schema
    primary_keys = primary keys for the table
    replication_keys = datetime keys for replication
    """

    name = "AdAnalyticsByCampaignInit"
    replication_keys = ["day"]
    replication_method = "incremental"
    primary_keys = ["campaign_id", "day"]
    path = "adAnalytics"

    schema = PropertiesList(
        Property("campaign_id", IntegerType),
        Property("documentCompletions", IntegerType),
        Property("documentFirstQuartileCompletions", IntegerType),
        Property("clicks", IntegerType),
        Property("documentMidpointCompletions", IntegerType),
        Property("documentThirdQuartileCompletions", IntegerType),
        Property("downloadClicks", IntegerType),
        Property("jobApplications", StringType),
        Property("jobApplyClicks", StringType),
        Property("postViewJobApplications", StringType),
        Property("costInUsd", StringType),
        Property("postViewRegistrations", StringType),
        Property("registrations", StringType),
        Property("talentLeads", IntegerType),
        Property("viralDocumentCompletions", IntegerType),
        Property("viralDocumentFirstQuartileCompletions", IntegerType),
        Property("viralDocumentMidpointCompletions", IntegerType),
        Property("viralDocumentThirdQuartileCompletions", IntegerType),
        Property("viralDownloadClicks", IntegerType),
        Property("viralJobApplications", StringType),
        Property("viralJobApplyClicks", StringType),
        Property("costInLocalCurrency", StringType),
        Property("viralRegistrations", StringType),
        Property("approximateUniqueImpressions", IntegerType),
        Property("cardClicks", IntegerType),
        Property("cardImpressions", IntegerType),
        Property("commentLikes", IntegerType),
        Property("viralCardClicks", IntegerType),
        Property("viralCardImpressions", IntegerType),
        Property("viralCommentLikes", IntegerType),
        Property("actionClicks", IntegerType),
        Property("adUnitClicks", IntegerType),
        Property("comments", IntegerType),
        Property("companyPageClicks", IntegerType),
        Property("conversionValueInLocalCurrency", StringType),
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
        Property("day", StringType),
        Property("externalWebsiteConversions", IntegerType),
        Property("externalWebsitePostClickConversions", IntegerType),
        Property("externalWebsitePostViewConversions", IntegerType),
        Property("follows", IntegerType),
        Property("fullScreenPlays", IntegerType),
        Property("impressions", IntegerType),
        Property("landingPageClicks", IntegerType),
        Property("leadGenerationMailContactInfoShares", IntegerType),
        Property("leadGenerationMailInterestedClicks", IntegerType),
        Property("likes", IntegerType),
        Property("oneClickLeadFormOpens", IntegerType),
        Property("oneClickLeads", IntegerType),
        Property("opens", IntegerType),
        Property("otherEngagements", IntegerType),
        Property("sends", IntegerType),
        Property("shares", IntegerType),
        Property("textUrlClicks", IntegerType),
        Property("totalEngagements", IntegerType),
        Property("videoCompletions", IntegerType),
        Property("videoFirstQuartileCompletions", IntegerType),
        Property("videoMidpointCompletions", IntegerType),
        Property("videoStarts", IntegerType),
        Property("videoThirdQuartileCompletions", IntegerType),
        Property("videoViews", IntegerType),
        Property("viralClicks", IntegerType),
        Property("viralComments", IntegerType),
        Property("viralCompanyPageClicks", IntegerType),
        Property("viralExternalWebsiteConversions", IntegerType),
        Property("viralExternalWebsitePostClickConversions", IntegerType),
        Property("viralExternalWebsitePostViewConversions", IntegerType),
        Property("viralFollows", IntegerType),
        Property("viralFullScreenPlays", IntegerType),
        Property("viralImpressions", IntegerType),
        Property("viralLandingPageClicks", IntegerType),
        Property("viralLikes", IntegerType),
        Property("viralOneClickLeadFormOpens", IntegerType),
        Property("viralOneclickLeads", IntegerType),
        Property("viralOtherEngagements", IntegerType),
        Property("viralReactions", IntegerType),
        Property("reactions", IntegerType),
        Property("viralShares", IntegerType),
        Property("viralTotalEngagements", IntegerType),
        Property("viralVideoCompletions", IntegerType),
        Property("viralVideoFirstQuartileCompletions", IntegerType),
        Property("viralVideoMidpointCompletions", IntegerType),
        Property("viralVideoStarts", IntegerType),
        Property("viralVideoThirdQuartileCompletions", IntegerType),
        Property("viralVideoViews", IntegerType),
    ).to_dict()

    @property
    def adanalyticscolumns(self) -> list[str]:
        return [
            "viralLandingPageClicks,viralExternalWebsitePostClickConversions,externalWebsiteConversions,viralVideoFirstQuartileCompletions,leadGenerationMailContactInfoShares,clicks,viralClicks,shares,viralFullScreenPlays,videoMidpointCompletions,viralCardClicks,viralExternalWebsitePostViewConversions,viralTotalEngagements,viralCompanyPageClicks,actionClicks,viralShares,videoCompletions,comments,externalWebsitePostViewConversions,dateRange",
            "costInUsd,landingPageClicks,oneClickLeadFormOpens,talentLeads,sends,viralOneClickLeadFormOpens,conversionValueInLocalCurrency,viralFollows,otherEngagements,viralVideoCompletions,cardImpressions,leadGenerationMailInterestedClicks,opens,totalEngagements,videoViews,viralImpressions,viralVideoViews,commentLikes,viralDocumentThirdQuartileCompletions,viralLikes",
            "adUnitClicks,videoThirdQuartileCompletions,cardClicks,likes,viralComments,viralVideoMidpointCompletions,viralVideoThirdQuartileCompletions,oneClickLeads,fullScreenPlays,viralCardImpressions,follows,videoStarts,videoFirstQuartileCompletions,textUrlClicks,reactions,viralReactions,externalWebsitePostClickConversions,viralOtherEngagements,costInLocalCurrency",
            "viralVideoStarts,viralRegistrations,viralJobApplyClicks,viralJobApplications,jobApplications,jobApplyClicks,viralExternalWebsiteConversions,postViewRegistrations,companyPageClicks,documentCompletions,documentFirstQuartileCompletions,documentMidpointCompletions,documentThirdQuartileCompletions,downloadClicks,viralDocumentCompletions,viralDocumentFirstQuartileCompletions,viralDocumentMidpointCompletions,approximateUniqueImpressions,viralDownloadClicks,impressions",
        ]

    def get_url_params(
        self,
        context: dict | None,  # noqa: ARG002
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        columns = self.adanalyticscolumns

        params: dict = {}
        if next_page_token:
            params["start"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key

        start_date = pendulum.parse(self.config["start_date"])
        end_date = pendulum.parse(self.config["end_date"])

        params["q"] = "analytics"
        params["pivot"] = "CAMPAIGN"
        params["timeGranularity"] = "DAILY"
        params["dateRange.start.day"] = start_date.day
        params["dateRange.start.month"] = start_date.month
        params["dateRange.start.year"] = start_date.year
        params["dateRange.end.day"] = end_date.day
        params["dateRange.end.month"] = end_date.month
        params["dateRange.end.year"] = end_date.year

        params["fields"] = columns[0]
        params["campaigns[0]"] = "urn:li:sponsoredCampaign:" + self.config["campaign"]

        return params

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        # This function extracts day, month, and year from date rannge column
        # These values are aprsed with datetime function and the date is added to the day column
        date_range = row.get("dateRange", {})
        start_date = date_range.get("start", {})

        if start_date:
            row["day"] = datetime.strptime(
                "{}-{}-{}".format(
                    start_date.get("year"),
                    start_date.get("month"),
                    start_date.get("day"),
                ),
                "%Y-%m-%d",
            ).astimezone(UTC)
            
        try:
            row["campaign_id"] = os.getenv("TAP_LINKEDIN_ADS_CAMPAIGN")# campaign_column
        except IndexError:
            pass

        return super().post_process(row, context)


class AdAnalyticsByCampaign(AdAnalyticsByCampaignInit):
    name = "ad_analytics_by_campaign"

    def get_url_params(
        self,
        context: dict | None,  # noqa: ARG002
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        columns = self.adanalyticscolumns

        params: dict = {}
        if next_page_token:
            params["start"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key

        start_date = pendulum.parse(self.config["start_date"])
        end_date = pendulum.parse(self.config["end_date"])

        params["q"] = "analytics"
        params["pivot"] = "CAMPAIGN"
        params["timeGranularity"] = "DAILY"
        params["dateRange.start.day"] = start_date.day
        params["dateRange.start.month"] = start_date.month
        params["dateRange.start.year"] = start_date.year
        params["dateRange.end.day"] = end_date.day
        params["dateRange.end.month"] = end_date.month
        params["dateRange.end.year"] = end_date.year
        params["fields"] = columns[1]
        params["campaigns[0]"] = "urn:li:sponsoredCampaign:" + self.config["campaign"]

        return params

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a dictionary of records from adAanalytics classes.

        Combines request columns from multiple calls to the api, which are limited to 20 columns
        each.

        Uses `merge_dicts` to combine responses from each class
        super().get_records calls only the records from the adAnalyticsByCampaign class
        zip() Iterates over the records of adAnalytics classes and merges them with merge_dicts()
        function list() converts each stream context into lists

        Args:
            context: The stream context.

        Returns:
            A dictionary of records given from adAnalytics streams
        """
        adanalyticsinit_stream = AdAnalyticsByCampaignInit(
            self._tap,
            schema={"properties": {}},
        )
        adanalyticsecond_stream = AdAnalyticsByCampaignSecond(
            self._tap,
            schema={"properties": {}},
        )
        adanalyticsthird_stream = AdAnalyticsByCampaignThird(
            self._tap,
            schema={"properties": {}},
        )
        return [
            self.merge_dicts(x, y, z, p)
            for x, y, z, p in zip(
                list(adanalyticsinit_stream.get_records(context)),
                list(super().get_records(context)),
                list(adanalyticsecond_stream.get_records(context)),
                list(adanalyticsthird_stream.get_records(context)),
            )
        ]

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


class AdAnalyticsByCampaignSecond(AdAnalyticsByCampaignInit):
    name = "adanalyticsbycampaign_second"

    def get_url_params(
        self,
        context: dict | None,  # noqa: ARG002
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        columns = self.adanalyticscolumns

        params: dict = {}
        if next_page_token:
            params["start"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key

        start_date = pendulum.parse(self.config["start_date"])
        end_date = pendulum.parse(self.config["end_date"])

        params["q"] = "analytics"
        params["pivot"] = "CAMPAIGN"
        params["timeGranularity"] = "DAILY"
        params["dateRange.start.day"] = start_date.day
        params["dateRange.start.month"] = start_date.month
        params["dateRange.start.year"] = start_date.year
        params["dateRange.end.day"] = end_date.day
        params["dateRange.end.month"] = end_date.month
        params["dateRange.end.year"] = end_date.year
        params["fields"] = columns[2]
        params["campaigns[0]"] = "urn:li:sponsoredCampaign:" + self.config["campaign"]

        return params


class AdAnalyticsByCampaignThird(AdAnalyticsByCampaignInit):
    name = "adanalyticsbycampaign_third"

    def get_url_params(
        self,
        context: dict | None,  # noqa: ARG002
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        columns = self.adanalyticscolumns

        params: dict = {}
        if next_page_token:
            params["start"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key

        start_date = pendulum.parse(self.config["start_date"])
        end_date = pendulum.parse(self.config["end_date"])

        params["q"] = "analytics"
        params["pivot"] = "CAMPAIGN"
        params["timeGranularity"] = "DAILY"
        params["dateRange.start.day"] = start_date.day
        params["dateRange.start.month"] = start_date.month
        params["dateRange.start.year"] = start_date.year
        params["dateRange.end.day"] = end_date.day
        params["dateRange.end.month"] = end_date.month
        params["dateRange.end.year"] = end_date.year
        params["fields"] = columns[3]
        params["campaigns[0]"] = "urn:li:sponsoredCampaign:" + self.config["campaign"]
        return params


class VideoAds(LinkedInAdsStream):
    """https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/advertising-targeting/create-and-manage-video#finders."""

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
        context: dict | None,  # noqa: ARG002
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
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
        params["account"] = "urn:li:sponsoredAccount:" + self.config["accounts"]
        params["owner"] = "urn:li:organization:" + self.config["owner"]

        return params


class AccountUsers(LinkedInAdsStream):
    """https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-account-users#find-ad-account-users-by-accounts."""

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
    replication_keys = ["user_person_id"]
    replication_method = "incremental"
    primary_keys = ["user_person_id", "last_modified_time"]
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
        context: dict | None,  # noqa: ARG002
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
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
        params["accounts"] = "urn:li:sponsoredAccount:" + self.config["accounts"]

        return params


class CampaignGroups(LinkedInAdsStream):
    """https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-campaign-groups#search-for-campaign-groups."""

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
    primary_keys = ["last_modified_time", "id", "status"]
    path = "adAccounts/{}/adCampaignGroups/{}".format(os.getenv("TAP_LINKEDIN_ADS_ACCOUNTS"), os.getenv("TAP_LINKEDIN_ADS_CAMPAIGN_GROUP"))

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
        context: dict | None,  # noqa: ARG002
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}

        return params


class Campaigns(LinkedInAdsStream):
    """https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-campaigns#search-for-campaigns."""

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
    primary_keys = ["last_modified_time", "id", "status"]
    #path = "adAccounts/510799602/adCampaigns/211290954"
    path = "adAccounts/{}/adCampaigns/{}".format(os.getenv("TAP_LINKEDIN_ADS_ACCOUNTS"), os.getenv("TAP_LINKEDIN_ADS_CAMPAIGN"))

    schema = PropertiesList(
        Property("storyDeliveryEnabled", BooleanType),
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
                            ObjectType(
                                Property(
                                    "or",
                                    ObjectType(
                                        Property(
                                            "urn:li:adTargetingFacet",
                                            th.ArrayType(
                                                Property("urn:li:title", StringType),
                                            ),
                                        ),
                                        Property(
                                            "urn:li:adTargetingFacet",
                                            th.ArrayType(
                                                Property("urn:li:geo", StringType),
                                            ),
                                        ),
                                        Property(
                                            "urn:li:adTargetingFacet",
                                            th.ArrayType(
                                                Property(
                                                    "urn:li:adSlotSize",
                                                    StringType,
                                                ),
                                            ),
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
        context: dict | None,  # noqa: ARG002
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}

        return params


class Creatives(LinkedInAdsStream):
    """https://learn.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-creatives?view=li-lms-2023-05&tabs=http%2Chttp-update-a-creative#search-for-creatives.
    columns: columns which will be added to fields parameter in api
    name: stream name
    path: path which will be added to api url in client.py
    schema: instream schema
    primary_keys = primary keys for the table
    replication_keys = datetime keys for replication
    """

    name = "creatives"
    replication_keys = ["lastModifiedAt"]
    replication_method = "incremental"
    primary_keys = ["lastModifiedAt", "id"]
    #path = "adAccounts/510799602/creatives/urn%3Ali%3AsponsoredCreative%3A204930534"
    path = "adAccounts/{}/creatives/urn%3Ali%3AsponsoredCreative%3A{}".format(os.getenv("TAP_LINKEDIN_ADS_ACCOUNTS"), os.getenv("TAP_LINKEDIN_ADS_CREATIVE"))

    schema = PropertiesList(
        Property("account", StringType),
        Property("account_id", IntegerType),
        Property("campaign", StringType),
        Property("campaign_id", IntegerType),
        Property(
            "content",
            ObjectType(
                Property(
                    "spotlight",
                    ObjectType(
                        Property("showMemberProfilePhoto", BooleanType),
                        Property("organizationName", StringType),
                        Property("landingPage", StringType),
                        Property("description", StringType),
                        Property("logo", StringType),
                        Property("headline", StringType),
                        Property("callToAction", StringType),
                        additional_properties=False,
                    ),
                ),
            ),
        ),
        Property("createdAt", StringType),
        Property("createdBy", StringType),
        Property("lastModifiedAt", StringType),
        Property("lastModifiedBy", StringType),
        Property("id", StringType),
        Property("intendedStatus", StringType),
        Property("isServing", BooleanType),
        Property("isTest", BooleanType),
        Property("servingHoldReasons", th.ArrayType(Property("items", StringType))),
    ).to_dict()

    def get_url_params(
        self,
        context: dict | None,  # noqa: ARG002
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}

        return params


class AdAnalyticsByCreativeInit(LinkedInAdsStream):
    """https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting#analytics-finder."""

    """
    columns: columns which will be added to fields parameter in api
    name: stream name
    path: path which will be added to api url in client.py
    schema: instream schema
    primary_keys = primary keys for the table
    replication_keys = datetime keys for replication
    """

    name = "AdAnalyticsByCreativeInit"
    replication_keys = ["dateRange"]
    replication_method = "incremental"
    primary_keys = ["creative_id", "dateRange"]
    path = "adAnalytics"

    schema = PropertiesList(
        Property("landingPageClicks", IntegerType),
        Property("reactions", IntegerType),
        Property("adUnitClicks", IntegerType),
        Property("creative_id", IntegerType),
        Property("documentCompletions", IntegerType),
        Property("documentFirstQuartileCompletions", IntegerType),
        Property("clicks", IntegerType),
        Property("documentMidpointCompletions", IntegerType),
        Property("documentThirdQuartileCompletions", IntegerType),
        Property("downloadClicks", IntegerType),
        Property("jobApplications", StringType),
        Property("jobApplyClicks", StringType),
        Property("postViewJobApplications", StringType),
        Property("costInUsd", StringType),
        Property("postViewRegistrations", StringType),
        Property("registrations", StringType),
        Property("talentLeads", IntegerType),
        Property("viralDocumentCompletions", IntegerType),
        Property("viralDocumentFirstQuartileCompletions", IntegerType),
        Property("viralDocumentMidpointCompletions", IntegerType),
        Property("viralDocumentThirdQuartileCompletions", IntegerType),
        Property("viralDownloadClicks", IntegerType),
        Property("viralJobApplications", StringType),
        Property("viralJobApplyClicks", StringType),
        Property("costInLocalCurrency", StringType),
        Property("viralRegistrations", IntegerType),
        Property("approximateUniqueImpressions", IntegerType),
        Property("cardClicks", IntegerType),
        Property("cardImpressions", IntegerType),
        Property("commentLikes", IntegerType),
        Property("viralCardClicks", IntegerType),
        Property("viralCardImpressions", IntegerType),
        Property("viralCommentLikes", IntegerType),
        Property("actionClicks", IntegerType),
        Property("comments", IntegerType),
        Property("companyPageClicks", IntegerType),
        Property("conversionValueInLocalCurrency", StringType),
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
        Property("day", StringType),
        Property("externalWebsiteConversions", IntegerType),
        Property("externalWebsitePostClickConversions", IntegerType),
        Property("externalWebsitePostViewConversions", IntegerType),
        Property("follows", IntegerType),
        Property("fullScreenPlays", IntegerType),
        Property("impressions", IntegerType),
        Property("landingPageClicks", IntegerType),
        Property("leadGenerationMailContactInfoShares", IntegerType),
        Property("leadGenerationMailInterestedClicks", IntegerType),
        Property("likes", IntegerType),
        Property("oneClickLeadFormOpens", IntegerType),
        Property("oneClickLeads", IntegerType),
        Property("opens", IntegerType),
        Property("otherEngagements", IntegerType),
        Property("sends", IntegerType),
        Property("shares", IntegerType),
        Property("textUrlClicks", IntegerType),
        Property("totalEngagements", IntegerType),
        Property("videoCompletions", IntegerType),
        Property("videoFirstQuartileCompletions", IntegerType),
        Property("videoMidpointCompletions", IntegerType),
        Property("videoStarts", IntegerType),
        Property("videoThirdQuartileCompletions", IntegerType),
        Property("videoViews", IntegerType),
        Property("viralClicks", IntegerType),
        Property("viralComments", IntegerType),
        Property("viralCompanyPageClicks", IntegerType),
        Property("viralExternalWebsiteConversions", IntegerType),
        Property("viralExternalWebsitePostClickConversions", IntegerType),
        Property("viralExternalWebsitePostViewConversions", IntegerType),
        Property("viralFollows", IntegerType),
        Property("viralFullScreenPlays", IntegerType),
        Property("viralImpressions", IntegerType),
        Property("viralLandingPageClicks", IntegerType),
        Property("viralLikes", IntegerType),
        Property("viralOneClickLeadFormOpens", IntegerType),
        Property("viralOneclickLeads", IntegerType),
        Property("viralOtherEngagements", IntegerType),
        Property("viralReactions", IntegerType),
        Property("viralShares", IntegerType),
        Property("viralTotalEngagements", IntegerType),
        Property("viralVideoCompletions", IntegerType),
        Property("viralVideoFirstQuartileCompletions", IntegerType),
        Property("viralVideoMidpointCompletions", IntegerType),
        Property("viralVideoStarts", IntegerType),
        Property("viralVideoThirdQuartileCompletions", IntegerType),
        Property("viralVideoViews", IntegerType),
    ).to_dict()

    @property
    def adanalyticscolumns(self) -> list[str]:
        """List of columns for adanalytics endpoint."""
        return [
            "viralLandingPageClicks,viralExternalWebsitePostClickConversions,externalWebsiteConversions,viralVideoFirstQuartileCompletions,leadGenerationMailContactInfoShares,clicks,viralClicks,shares,viralFullScreenPlays,videoMidpointCompletions,viralCardClicks,viralExternalWebsitePostViewConversions,viralTotalEngagements,viralCompanyPageClicks,actionClicks,viralShares,videoCompletions,comments,externalWebsitePostViewConversions,dateRange",
            "costInUsd,landingPageClicks,oneClickLeadFormOpens,talentLeads,sends,viralOneClickLeadFormOpens,conversionValueInLocalCurrency,viralFollows,otherEngagements,viralVideoCompletions,cardImpressions,leadGenerationMailInterestedClicks,opens,totalEngagements,videoViews,viralImpressions,viralVideoViews,commentLikes,viralDocumentThirdQuartileCompletions,viralLikes",
            "adUnitClicks,videoThirdQuartileCompletions,cardClicks,likes,viralComments,viralVideoMidpointCompletions,viralVideoThirdQuartileCompletions,oneClickLeads,fullScreenPlays,viralCardImpressions,follows,videoStarts,videoFirstQuartileCompletions,textUrlClicks,reactions,viralReactions,externalWebsitePostClickConversions,viralOtherEngagements,costInLocalCurrency",
            "viralVideoStarts,viralRegistrations,viralJobApplyClicks,viralJobApplications,jobApplications,jobApplyClicks,viralExternalWebsiteConversions,postViewRegistrations,companyPageClicks,documentCompletions,documentFirstQuartileCompletions,documentMidpointCompletions,documentThirdQuartileCompletions,downloadClicks,viralDocumentCompletions,viralDocumentFirstQuartileCompletions,viralDocumentMidpointCompletions,approximateUniqueImpressions,viralDownloadClicks,impressions",
        ]

    def get_url_params(
        self,
        context: dict | None,  # noqa: ARG002
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        columns = self.adanalyticscolumns

        params: dict = {}
        if next_page_token:
            params["start"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key

        params["fields"] = columns[0]

        start_date = pendulum.parse(self.config["start_date"])
        end_date = pendulum.parse(self.config["end_date"])

        params["q"] = "analytics"
        params["pivot"] = "CREATIVE"
        params["timeGranularity"] = "DAILY"
        params["dateRange.start.day"] = start_date.day
        params["dateRange.start.month"] = start_date.month
        params["dateRange.start.year"] = start_date.year
        params["dateRange.end.day"] = end_date.day
        params["dateRange.end.month"] = end_date.month
        params["dateRange.end.year"] = end_date.year
        params["campaigns[0]"] = "urn:li:sponsoredCampaign:" + self.config["campaign"]

        return params

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        # This function extracts day, month, and year from date rannge column
        # These values are aprsed with datetime function and the date is added to the day column
        date_range = row.get("dateRange", {})
        start_date = date_range.get("start", {})

        if start_date:
            row["day"] = datetime.strptime(
                "{}-{}-{}".format(
                    start_date.get("year"),
                    start_date.get("month"),
                    start_date.get("day"),
                ),
                "%Y-%m-%d",
            ).astimezone(UTC)


        try:
            row["creative_id"] = os.getenv("TAP_LINKEDIN_ADS_CREATIVE")
        except IndexError:
            pass

        return super().post_process(row, context)


class AdAnalyticsByCreative(AdAnalyticsByCreativeInit):
    name = "ad_analytics_by_creative"

    def get_url_params(
        self,
        context: dict | None,  # noqa: ARG002
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        columns = self.adanalyticscolumns

        params: dict = {}
        if next_page_token:
            params["start"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key

        start_date = pendulum.parse(self.config["start_date"])
        end_date = pendulum.parse(self.config["end_date"])

        params["q"] = "analytics"
        params["pivot"] = "CREATIVE"
        params["timeGranularity"] = "DAILY"
        params["dateRange.start.day"] = start_date.day
        params["dateRange.start.month"] = start_date.month
        params["dateRange.start.year"] = start_date.year
        params["dateRange.end.day"] = end_date.day
        params["dateRange.end.month"] = end_date.month
        params["dateRange.end.year"] = end_date.year
        params["fields"] = columns[1]
        params["campaigns[0]"] = "urn:li:sponsoredCampaign:" + self.config["campaign"]

        return params

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a dictionary of records from adAnalytics classes.

        Combines request columns from multiple calls to the api, which are limited to 20 columns
        each.

        Uses `merge_dicts` to combine responses from each class
        super().get_records calls only the records from adAnalyticsByCreative class
        zip() Iterates over the records of adAnalytics classes and merges them with merge_dicts()
        function list() converts each stream context into lists

        Args:
            context: The stream context.

        Returns:
            A dictionary of records given from adAnalytics streams
        """
        adanalyticsinit_stream = AdAnalyticsByCreativeInit(
            self._tap,
            schema={"properties": {}},
        )
        adanalyticsecond_stream = AdAnalyticsByCreativeSecond(
            self._tap,
            schema={"properties": {}},
        )
        adanalyticsthird_stream = AdAnalyticsByCreativeThird(
            self._tap,
            schema={"properties": {}},
        )
        return [
            self.merge_dicts(x, y, z, p)
            for x, y, z, p in zip(
                list(adanalyticsinit_stream.get_records(context)),
                list(super().get_records(context)),
                list(adanalyticsecond_stream.get_records(context)),
                list(adanalyticsthird_stream.get_records(context)),
            )
        ]

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


class AdAnalyticsByCreativeSecond(AdAnalyticsByCreativeInit):
    name = "adanalyticsbycreative_second"

    def get_url_params(
        self,
        context: dict | None,  # noqa: ARG002
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        columns = self.adanalyticscolumns

        params: dict = {}
        if next_page_token:
            params["start"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key

        start_date = pendulum.parse(self.config["start_date"])
        end_date = pendulum.parse(self.config["end_date"])

        params["q"] = "analytics"
        params["pivot"] = "CREATIVE"
        params["timeGranularity"] = "DAILY"
        params["dateRange.start.day"] = start_date.day
        params["dateRange.start.month"] = start_date.month
        params["dateRange.start.year"] = start_date.year
        params["dateRange.end.day"] = end_date.day
        params["dateRange.end.month"] = end_date.month
        params["dateRange.end.year"] = end_date.year
        params["fields"] = columns[2]
        params["campaigns[0]"] = "urn:li:sponsoredCampaign:" + self.config["campaign"]

        return params


class AdAnalyticsByCreativeThird(AdAnalyticsByCreativeInit):
    name = "adanalyticsbycreative_third"

    def get_url_params(
        self,
        context: dict | None,  # noqa: ARG002
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        columns = self.adanalyticscolumns

        params: dict = {}
        if next_page_token:
            params["start"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key

        start_date = pendulum.parse(self.config["start_date"])
        end_date = pendulum.parse(self.config["end_date"])

        params["q"] = "analytics"
        params["pivot"] = "CREATIVE"
        params["timeGranularity"] = "DAILY"
        params["dateRange.start.day"] = start_date.day
        params["dateRange.start.month"] = start_date.month
        params["dateRange.start.year"] = start_date.year
        params["dateRange.end.day"] = end_date.day
        params["dateRange.end.month"] = end_date.month
        params["dateRange.end.year"] = end_date.year
        params["fields"] = columns[3]
        params["campaigns[0]"] = "urn:li:sponsoredCampaign:" + self.config["campaign"]

        return params
