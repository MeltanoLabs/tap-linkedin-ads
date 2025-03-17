"""Stream type classes for tap-linkedin-ads."""

from __future__ import annotations

import typing as t
from datetime import datetime, timezone
from importlib import resources

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

from tap_linkedin_ads.streams.base_stream import LinkedInAdsStreamBase

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context
from singer_sdk.streams.core import REPLICATION_INCREMENTAL

SCHEMAS_DIR = resources.files(__package__) / "schemas"
UTC = timezone.utc


class LinkedInAdsStream(LinkedInAdsStreamBase):
    """LinkedInAds stream class."""

    replication_key = "last_modified_time"
    # Note: manually filtering in post_process since the API doesnt have filter options
    replication_method = REPLICATION_INCREMENTAL

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        """Post-process each record returned by the API."""
        if "changeAuditStamps" in row:
            created_time = (
                row.get("changeAuditStamps", {}).get("created", {}).get("time")
            )
            last_modified_time = (
                row.get("changeAuditStamps", {}).get("lastModified", {}).get("time")
            )
            row["created_time"] = datetime.fromtimestamp(
                int(created_time) / 1000,
                tz=UTC,
            ).isoformat()
            row["last_modified_time"] = datetime.fromtimestamp(
                int(last_modified_time) / 1000,
                tz=UTC,
            ).isoformat()
        elif "createdAt" in row:
            row["created_time"] = datetime.fromtimestamp(
                int(row["createdAt"]) / 1000,
                tz=UTC,
            ).isoformat()
            row["last_modified_time"] = datetime.fromtimestamp(
                int(row["lastModifiedAt"]) / 1000,
                tz=UTC,
            ).isoformat()
        else:
            msg = "No changeAuditStamps or createdAt/lastModifiedAt fields found"
            raise Exception(msg)  # noqa: TRY002
        # Manual date filtering
        date = datetime.fromisoformat(row["last_modified_time"])
        start_date = self.get_starting_timestamp(context)
        end_date = datetime.fromisoformat(self.config["end_date"]).replace(
            tzinfo=timezone.utc
        )
        if date >= start_date and date <= end_date:
            return super().post_process(row, context)
        return None


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
        Property("created_time", DateTimeType),
        Property("last_modified_time", DateTimeType),
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

    def get_child_context(self, record: dict, context: dict | None) -> dict:  # noqa: ARG002
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
        Property("created_time", DateTimeType),
        Property("last_modified_time", DateTimeType),
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

    def get_unencoded_params(self, context: Context) -> dict:
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
        if not context:
            msg = "Context is required for this stream"
            raise ValueError(msg)
        return super().get_url(context) + f"/{context['account_id']}/adCampaigns"

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

    def get_unencoded_params(self, context: Context) -> dict:  # noqa: ARG002
        """Return a dictionary of unencoded params.

        Args:
            context: The stream context.

        Returns:
            A dictionary of URL query parameters.
        """
        return {
            "search": (
                "(status:(values:List(ACTIVE,PAUSED,ARCHIVED,COMPLETED,"
                "CANCELED,DRAFT,PENDING_DELETION,REMOVED)))"
            )
        }

    def get_child_context(self, record: dict, context: dict | None) -> dict:  # noqa: ARG002
        """Return a context dictionary for a child stream."""
        return {
            "campaign_id": record["id"],
        }

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        """Post-process each record returned by the API."""
        row["run_schedule_start"] = datetime.fromtimestamp(  # noqa: DTZ006
            int(row["runSchedule"]["start"]) / 1000,
        ).isoformat()
        row["campaign_group_id"] = int(row["campaignGroup"].split(":")[3])
        return super().post_process(row, context)


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
        if not context:
            msg = "Context is required for this stream"
            raise ValueError(msg)
        return super().get_url(context) + f"/{context['account_id']}/adCampaignGroups"

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

    def get_unencoded_params(self, context: Context) -> dict:  # noqa: ARG002
        """Return a dictionary of unencoded params.

        Args:
            context: The stream context.

        Returns:
            A dictionary of URL query parameters.
        """
        return {
            "search": (
                "(status:(values:List(ACTIVE,ARCHIVED,CANCELED,DRAFT,PAUSED,"
                "PENDING_DELETION,REMOVED)))"
            )
        }

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        """Post-process each record returned by the API."""
        row["run_schedule_start"] = datetime.fromtimestamp(  # noqa: DTZ006
            int(row["runSchedule"]["start"]) / 1000,
        ).isoformat()
        return super().post_process(row, context)


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
        Property("created_time", DateTimeType),
        Property("last_modified_time", DateTimeType),
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
        if not context:
            msg = "Context is required for this stream"
            raise ValueError(msg)
        return super().get_url(context) + f"/{context['account_id']}/creatives"

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

    def get_child_context(self, record: dict, context: dict | None) -> dict:  # noqa: ARG002
        """Return a context dictionary for a child stream."""
        creative_id = record["id"].split(":")[-1]
        return {
            "creative_id": creative_id,
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
        Property("created_time", DateTimeType),
        Property("last_modified_time", DateTimeType),
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
        if not context:
            msg = "Context is required for this stream"
            raise ValueError(msg)
        return {
            "q": "account",
            "account": f"urn:li:sponsoredAccount:{context['account_id']}",
            "owner": context["owner_urn"],
            **super().get_url_params(context, next_page_token),
        }
