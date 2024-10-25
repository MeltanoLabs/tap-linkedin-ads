"""Stream type classes for tap-linkedin-ads."""

from __future__ import annotations

import typing as t
from datetime import timezone
from importlib import resources

import pendulum
from singer_sdk.helpers.types import Context
from singer_sdk.typing import (
    IntegerType,
    ObjectType,
    PropertiesList,
    Property,
    StringType,
)

from tap_linkedin_ads.streams.ad_analytics.ad_analytics_base import AdAnalyticsBase
from tap_linkedin_ads.streams.streams import CampaignsStream

SCHEMAS_DIR = resources.files(__package__) / "schemas"
UTC = timezone.utc


class _AdAnalyticsByCampaignInit(AdAnalyticsBase):
    """https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting#analytics-finder."""

    name = "AdAnalyticsByCampaignInit"
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
        start_date = self.get_starting_timestamp(context)
        end_date = pendulum.parse(self.config["end_date"])
        return {
            "pivot": "(value:CAMPAIGN)",
            "timeGranularity": "(value:DAILY)",
            "campaigns": f"List(urn%3Ali%3AsponsoredCampaign%3A{context['campaign_id']})",
            "dateRange": f"(start:(year:{start_date.year},month:{start_date.month},day:{start_date.day}),end:(year:{end_date.year},month:{end_date.month},day:{end_date.day}))",
            "fields": self.adanalyticscolumns[0],
        }


class _AdAnalyticsByCampaignSecond(_AdAnalyticsByCampaignInit):
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


class _AdAnalyticsByCampaignThird(_AdAnalyticsByCampaignInit):
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


class AdAnalyticsByCampaignStream(_AdAnalyticsByCampaignInit):
    """https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting#analytics-finder."""

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
        adanalyticsinit_stream = _AdAnalyticsByCampaignInit(
            self._tap,
            schema={"properties": {}},
        )
        adanalyticsecond_stream = _AdAnalyticsByCampaignSecond(
            self._tap,
            schema={"properties": {}},
        )
        adanalyticsthird_stream = _AdAnalyticsByCampaignThird(
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
