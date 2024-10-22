"""Stream type classes for tap-linkedin-ads."""

from __future__ import annotations

import typing as t
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

from tap_linkedin_ads.client import LinkedInAdsStream

SCHEMAS_DIR = resources.files(__package__) / "schemas"


class AccountsStream(LinkedInAdsStream):
    """https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-accounts#search-for-accounts."""

    name = "accounts"
    primary_keys = ["last_modified_time", "id", "status"]
    path = "/adAccounts"

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

    def get_url_params(
        self,
        context: dict | None,  # noqa: ARG002
        next_page_token: t.Any | None,  # noqa: ANN401
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params = super().get_url_params(context, next_page_token)

        params["q"] = "search"
        params["sortOrder"] = "ASCENDING"
        return params

    def get_child_context(self, record: dict, context: t.Optional[dict]) -> dict:
        """Return a context dictionary for a child stream."""
        return {"account_id": record["id"]}



class CampaignsStream(LinkedInAdsStream):
    """https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-campaigns#search-for-campaigns."""

    """
    columns: columns which will be added to fields parameter in api
    name: stream name
    path: path which will be added to api url in client.py
    schema: instream schema
    primary_keys = primary keys for the table
    replication_keys = datetime keys for replication
    """

    name = "campaigns"
    path = "/adAccounts"
    primary_keys = ["last_modified_time", "id", "status"]
    parent_stream_type = AccountsStream
    next_page_token_jsonpath = "$.metadata.nextPageToken"  # Or override `get_next_page_token`.  # noqa: S105
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
        self.logger.info(context["account_id"])
        return super().get_url(context) + f'/{context["account_id"]}/adCampaigns'

    def get_unescaped_params(self):
        return {
            "search": "(status:(values:List(ACTIVE,PAUSED,ARCHIVED,COMPLETED,CANCELED,DRAFT,PENDING_DELETION,REMOVED)))"
        }
