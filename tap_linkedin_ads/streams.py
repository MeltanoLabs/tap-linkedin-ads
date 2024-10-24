"""Stream type classes for tap-linkedin-ads."""

from __future__ import annotations

import typing as t
from datetime import datetime, timezone
from importlib import resources

import pendulum
from singer_sdk.helpers.types import Context
from singer_sdk.typing import (
    ArrayType,
    BooleanType,
    DateTimeType,
    IntegerType,
    ObjectType,
    PropertiesList,
    Property,
    StringType,
)

from tap_linkedin_ads.client import LinkedInAdsStream

SCHEMAS_DIR = resources.files(__package__) / "schemas"
UTC = timezone.utc


class AccountsStream(LinkedInAdsStream):
    """https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-accounts#search-for-accounts."""

    name = "accounts"
    primary_keys: t.ClassVar[list[str]] = ["id"]

    schema = PropertiesList(
        Property(
            "changeAuditStamps",
            ObjectType(
                Property(
                    "created",
                    ObjectType(
                        Property("time", IntegerType),
                        additional_properties=False,
                    ),
                ),
                Property(
                    "lastModified",
                    ObjectType(
                        Property("time", IntegerType),
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
        Property("servingStatuses", ArrayType(Property("items", StringType))),
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

    def get_child_context(self, record: dict, context: t.Optional[dict]) -> dict:
        """Return a context dictionary for a child stream."""
        return {
            "account_id": record["id"],
            "owner_urn": record["reference"],
        }

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: t.Any | None,  # noqa: ANN401
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        return {
            "q": "search",
            "sortOrder": "ASCENDING",
            **super().get_url_params(context, next_page_token),
        }


class AccountUsersStream(LinkedInAdsStream):
    """https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-account-users#find-ad-account-users-by-accounts."""

    name = "account_users"
    parent_stream_type = AccountsStream
    primary_keys: t.ClassVar[list[str]] = ["account"]
    path = "/adAccountUsers"

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
                        Property("time", IntegerType),
                        additional_properties=False,
                    ),
                ),
                Property(
                    "lastModified",
                    ObjectType(
                        Property("time", IntegerType),
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
        next_page_token: t.Any | None,  # noqa: ANN401
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        return {
            "q": "accounts",
            **super().get_url_params(context, next_page_token),
        }

    def get_unencoded_params(self, context: Context | None) -> dict:
        """Return a dictionary of unencoded params.

        Args:
            context: The stream context.

        Returns:
            A dictionary of URL query parameters.
        """
        return {
            "accounts": f"urn:li:sponsoredAccount:{context['account_id']}",
        }


class CampaignsStream(LinkedInAdsStream):
    """https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-campaigns#search-for-campaigns."""

    name = "campaigns"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    parent_stream_type = AccountsStream
    next_page_token_jsonpath = (
        "$.metadata.nextPageToken"  # Or override `get_next_page_token`.  # noqa: S105
    )
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
                            ArrayType(
                                Property(
                                    "items",
                                    ObjectType(
                                        Property("type", StringType),
                                        Property(
                                            "values",
                                            ArrayType(Property("items", StringType)),
                                        ),
                                        additional_properties=False,
                                    ),
                                ),
                            ),
                        ),
                        Property(
                            "excluded_targeting_facets",
                            ArrayType(
                                Property(
                                    "items",
                                    ObjectType(
                                        Property("type", StringType),
                                        Property(
                                            "values",
                                            ArrayType(Property("items", StringType)),
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
                            ArrayType(
                                ObjectType(
                                    Property(
                                        "or",
                                        ObjectType(
                                            Property(
                                                "urn:li:adTargetingFacet",
                                                ArrayType(
                                                    Property(
                                                        "urn:li:title",
                                                        StringType,
                                                    ),
                                                ),
                                            ),
                                            Property(
                                                "urn:li:adTargetingFacet",
                                                ArrayType(
                                                    Property("urn:li:geo", StringType),
                                                ),
                                            ),
                                            Property(
                                                "urn:li:adTargetingFacet",
                                                ArrayType(
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
                ),
                Property(
                    "exclude",
                    ObjectType(
                        Property(
                            "or",
                            ObjectType(
                                Property(
                                    "urn:li:ad_targeting_facet:titles",
                                    ArrayType(
                                        Property("items", StringType),
                                    ),
                                ),
                                Property(
                                    "urn:li:ad_targeting_facet:staff_count_ranges",
                                    ArrayType(
                                        Property("items", StringType),
                                    ),
                                ),
                                Property(
                                    "urn:li:ad_targeting_facet:followed_companies",
                                    ArrayType(
                                        Property("items", StringType),
                                    ),
                                ),
                                Property(
                                    "urn:li:ad_targeting_facet:seniorities",
                                    ArrayType(
                                        Property("items", StringType),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        ),
        Property("servingStatuses", ArrayType(Property("items", StringType))),
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
                Property("start", IntegerType),
                Property("end", IntegerType),
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
                        Property("time", IntegerType),
                        additional_properties=False,
                    ),
                ),
                Property(
                    "lastModified",
                    ObjectType(
                        Property("time", IntegerType),
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
                            ArrayType(
                                Property("items", StringType),
                            ),
                        ),
                        Property(
                            "include",
                            ArrayType(Property("items", StringType)),
                        ),
                    ),
                ),
                Property(
                    "publisherRestrictionFiles",
                    ObjectType(
                        Property(
                            "exclude",
                            ArrayType(Property("items", StringType)),
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

    def get_url(self, context: dict | None) -> str:
        """Get stream entity URL.

        Developers override this method to perform dynamic URL generation.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            A URL, optionally targeted to a specific partition or context.
        """
        return super().get_url(context) + f'/{context["account_id"]}/adCampaigns'

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: t.Any | None,  # noqa: ANN401
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        return {
            "q": "search",
            "sortOrder": "ASCENDING",
            **super().get_url_params(context, next_page_token),
        }

    def get_unencoded_params(self, context: Context | None) -> dict:
        """Return a dictionary of unencoded params.

        Args:
            context: The stream context.

        Returns:
            A dictionary of URL query parameters.
        """
        return {
            "search": "(status:(values:List(ACTIVE,PAUSED,ARCHIVED,COMPLETED,CANCELED,DRAFT,PENDING_DELETION,REMOVED)))"
        }

    def get_child_context(self, record: dict, context: t.Optional[dict]) -> dict:
        """Return a context dictionary for a child stream."""
        return {
            "campaign_id": record["id"],
        }


class CampaignGroupsStream(LinkedInAdsStream):
    """https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-campaign-groups#search-for-campaign-groups."""

    name = "campaign_groups"
    parent_stream_type = AccountsStream
    primary_keys: t.ClassVar[list[str]] = ["id"]

    schema = PropertiesList(
        Property(
            "runSchedule",
            ObjectType(Property("start", IntegerType), Property("end", IntegerType)),
        ),
        Property(
            "changeAuditStamps",
            ObjectType(
                Property(
                    "created",
                    ObjectType(
                        Property("time", IntegerType),
                        additional_properties=False,
                    ),
                ),
                Property(
                    "lastModified",
                    ObjectType(
                        Property("time", IntegerType),
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

    def get_url(self, context: dict | None) -> str:
        """Get stream entity URL.

        Developers override this method to perform dynamic URL generation.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            A URL, optionally targeted to a specific partition or context.
        """
        return super().get_url(context) + f'/{context["account_id"]}/adCampaignGroups'

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: t.Any | None,  # noqa: ANN401
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        return {
            "q": "search",
            "sortOrder": "ASCENDING",
            **super().get_url_params(context, next_page_token),
        }

    def get_unencoded_params(self, context: Context | None) -> dict:
        """Return a dictionary of unencoded params.

        Args:
            context: The stream context.

        Returns:
            A dictionary of URL query parameters.
        """
        return {
            "search": "(status:(values:List(ACTIVE,ARCHIVED,CANCELED,DRAFT,PAUSED,PENDING_DELETION,REMOVED)))"
        }


class CreativesStream(LinkedInAdsStream):
    """https://learn.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-creatives?view=li-lms-2023-05&tabs=http%2Chttp-update-a-creative#search-for-creatives."""

    name = "creatives"
    parent_stream_type = AccountsStream
    primary_keys: t.ClassVar[list[str]] = ["id"]

    schema = PropertiesList(
        Property("account", StringType),
        Property("account_id", IntegerType),
        Property("campaign", StringType),
        Property("campaign_id", StringType),
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
        Property("createdAt", IntegerType),
        Property("createdBy", StringType),
        Property("lastModifiedAt", IntegerType),
        Property("lastModifiedBy", StringType),
        Property("id", StringType),
        Property("intendedStatus", StringType),
        Property("isServing", BooleanType),
        Property("isTest", BooleanType),
        Property("servingHoldReasons", ArrayType(Property("items", StringType))),
    ).to_dict()

    def get_url(self, context: dict | None) -> str:
        """Get stream entity URL.

        Developers override this method to perform dynamic URL generation.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            A URL, optionally targeted to a specific partition or context.
        """
        # TODO: optional filter 'urn%3Ali%3AsponsoredCreative%3A{self.config["creative"]}'
        return super().get_url(context) + f'/{context["account_id"]}/creatives'

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: t.Any | None,  # noqa: ANN401
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        return {
            "q": "criteria",
            **super().get_url_params(context, next_page_token),
        }

    def get_child_context(self, record: dict, context: t.Optional[dict]) -> dict:
        """Return a context dictionary for a child stream."""
        return {
            "creative_urn": record["id"],
        }


class VideoAdsStream(LinkedInAdsStream):
    """https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/advertising-targeting/create-and-manage-video#finders."""

    name = "video_ads"
    path = "/adDirectSponsoredContents"
    parent_stream_type = AccountsStream

    schema = PropertiesList(
        Property("account", StringType),
        Property("account_id", IntegerType),
        Property(
            "changeAuditStamps",
            ObjectType(
                Property(
                    "created",
                    ObjectType(
                        Property("time", IntegerType),
                        additional_properties=False,
                    ),
                ),
                Property(
                    "lastModified",
                    ObjectType(
                        Property("time", IntegerType),
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

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return "https://api.linkedin.com/v2"

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: t.Any | None,  # noqa: ANN401
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        return {
            "q": "account",
            "account": f"urn:li:sponsoredAccount:{context['account_id']}",
            "owner": context["owner_urn"],
            **super().get_url_params(context, next_page_token),
        }

    # TODO: handle timestamp parsing more generically
    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        # This function extracts day, month, and year from date range column
        # These values are parse with datetime function and the date is added to the day column
        # with contextlib.suppress(Exception):
        #     created_time = row.get("changeAuditStamps", {}).get("created", {}).get("time")
        #     last_modified_time = (
        #         row.get("changeAuditStamps", {}).get("lastModified", {}).get("time")
        #     )
        #     row["created_time"] = datetime.fromtimestamp(
        #         int(created_time) / 1000,
        #         tz=UTC,
        #     ).isoformat()
        #     row["last_modified_time"] = datetime.fromtimestamp(
        #         int(last_modified_time) / 1000,
        #         tz=UTC,
        #     ).isoformat()
        return super().post_process(row, context)


class AdAnalyticsByCampaignInit(LinkedInAdsStream):
    """https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting#analytics-finder."""

    name = "AdAnalyticsByCampaignInit"
    path = "/adAnalytics"
    parent_stream_type = CampaignsStream

    schema = PropertiesList(
        Property("campaign_id", StringType),
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
        context: dict | None,
        next_page_token: t.Any | None,  # noqa: ANN401
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        return {
            "q": "analytics",
            **super().get_url_params(context, next_page_token),
        }

    def get_unencoded_params(self, context: Context | None) -> dict:
        """Return a dictionary of unencoded params.

        Args:
            context: The stream context.

        Returns:
            A dictionary of URL query parameters.
        """
        start_date = pendulum.parse(self.config["start_date"])
        end_date = pendulum.parse(self.config["end_date"])
        return {
            "pivot": "(value:CAMPAIGN)",
            "timeGranularity": "(value:DAILY)",
            "campaigns": f"List(urn%3Ali%3AsponsoredCampaign%3A{context['campaign_id']})",
            "dateRange": f"(start:(year:{start_date.year},month:{start_date.month},day:{start_date.day}),end:(year:{end_date.year},month:{end_date.month},day:{end_date.day}))",
            "fields": self.adanalyticscolumns[0],
        }

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        # This function extracts day, month, and year from date range column
        # These values are parsed with datetime function and the date is added to the day column
        date_range = row.get("dateRange", {})
        start_date = date_range.get("start", {})

        if start_date:
            row["day"] = datetime.strptime(
                f'{start_date.get("year")}-{start_date.get("month")}-{start_date.get("day")}',
                "%Y-%m-%d",
            ).astimezone(UTC)

        return super().post_process(row, context)


class AdAnalyticsByCampaignSecond(AdAnalyticsByCampaignInit):
    name = "adanalyticsbycampaign_second"

    def get_unencoded_params(self, context: Context | None) -> dict:
        """Return a dictionary of unencoded params.

        Args:
            context: The stream context.

        Returns:
            A dictionary of URL query parameters.
        """
        return {
            **super().get_unencoded_params(context),
            # Overwrite fields with this column subset
            "fields": self.adanalyticscolumns[0],
        }


class AdAnalyticsByCampaignThird(AdAnalyticsByCampaignInit):
    name = "adanalyticsbycampaign_third"

    def get_unencoded_params(self, context: Context | None) -> dict:
        """Return a dictionary of unencoded params.

        Args:
            context: The stream context.

        Returns:
            A dictionary of URL query parameters.
        """
        return {
            **super().get_unencoded_params(context),
            # Overwrite fields with this column subset
            "fields": self.adanalyticscolumns[3],
        }


class AdAnalyticsByCampaignStream(AdAnalyticsByCampaignInit):
    name = "ad_analytics_by_campaign"

    def get_unencoded_params(self, context: Context | None) -> dict:
        """Return a dictionary of unencoded params.

        Args:
            context: The stream context.

        Returns:
            A dictionary of URL query parameters.
        """
        return {
            **super().get_unencoded_params(context),
            # Overwrite fields with this column subset
            "fields": self.adanalyticscolumns[1],
        }

    def get_records(self, context: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a dictionary of records from adAnalytics classes.

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


class AdAnalyticsByCreativeInit(LinkedInAdsStream):
    """https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting#analytics-finder."""

    name = "AdAnalyticsByCreativeInit"
    path = "/adAnalytics"
    parent_stream_type = CreativesStream

    schema = PropertiesList(
        Property("landingPageClicks", IntegerType),
        Property("reactions", IntegerType),
        Property("adUnitClicks", IntegerType),
        Property("creative_id", StringType),
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
        context: dict | None,
        next_page_token: t.Any | None,  # noqa: ANN401
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        return {
            "q": "analytics",
            **super().get_url_params(context, next_page_token),
        }

    def get_unencoded_params(self, context: Context | None) -> dict:
        """Return a dictionary of unencoded params.

        Args:
            context: The stream context.

        Returns:
            A dictionary of URL query parameters.
        """
        start_date = pendulum.parse(self.config["start_date"])
        end_date = pendulum.parse(self.config["end_date"])
        creative_urn = context["creative_urn"]
        creative_id = creative_urn.split(":")[-1]
        return {
            "pivot": "(value:CREATIVE)",
            "timeGranularity": "(value:DAILY)",
            "creatives": f"List(urn%3Ali%3AsponsoredCreative%3A{creative_id})",
            "dateRange": f"(start:(year:{start_date.year},month:{start_date.month},day:{start_date.day}),end:(year:{end_date.year},month:{end_date.month},day:{end_date.day}))",
            "fields": self.adanalyticscolumns[0],
        }

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        # This function extracts day, month, and year from date range column
        # These values are parsed with datetime function and the date is added to the day column
        date_range = row.get("dateRange", {})
        start_date = date_range.get("start", {})

        if start_date:
            row["day"] = datetime.strptime(
                f'{start_date.get("year")}-{start_date.get("month")}-{start_date.get("day")}',
                "%Y-%m-%d",
            ).astimezone(UTC)

        viral_registrations = row.pop("viralRegistrations", None)
        if viral_registrations:
            row["viralRegistrations"] = int(viral_registrations)

        return super().post_process(row, context)


class AdAnalyticsByCreativeStream(AdAnalyticsByCreativeInit):
    name = "ad_analytics_by_creative"

    def get_unencoded_params(self, context: Context | None) -> dict:
        """Return a dictionary of unencoded params.

        Args:
            context: The stream context.

        Returns:
            A dictionary of URL query parameters.
        """
        return {
            **super().get_unencoded_params(context),
            # Overwrite fields with this column subset
            "fields": self.adanalyticscolumns[1],
        }

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

    def get_unencoded_params(self, context: Context | None) -> dict:
        """Return a dictionary of unencoded params.

        Args:
            context: The stream context.

        Returns:
            A dictionary of URL query parameters.
        """
        return {
            **super().get_unencoded_params(context),
            # Overwrite fields with this column subset
            "fields": self.adanalyticscolumns[2],
        }


class AdAnalyticsByCreativeThird(AdAnalyticsByCreativeInit):
    name = "adanalyticsbycreative_third"

    def get_unencoded_params(self, context: Context | None) -> dict:
        """Return a dictionary of unencoded params.

        Args:
            context: The stream context.

        Returns:
            A dictionary of URL query parameters.
        """
        return {
            **super().get_unencoded_params(context),
            # Overwrite fields with this column subset
            "fields": self.adanalyticscolumns[3],
        }
