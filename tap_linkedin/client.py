"""REST client handling, including LinkedInStream base class."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Iterable

import requests
from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream

_Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class LinkedInStream(RESTStream):
    """LinkedIn stream class."""

    url_base = "https://api.linkedin.com/rest/"

    records_jsonpath = "$.elements[*]"  # Or override `parse_response`.
    next_page_token_jsonpath = "$.paging.start"  # Or override `get_next_page_token`.

    @property
    def authenticator(self) -> BearerTokenAuthenticator:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return BearerTokenAuthenticator.create_for_stream(
            self,
            token=self.config.get("access_token", ""),
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
            headers["LinkedIn-Version"] = self.config.get("linkedin_version")
            headers["X-Restli-Protocol-Version"]= self.config.get("x-restli-protocol-version")
            headers["Content-Type"] = self.config.get("application/json; charset=utf-8")

            
        # If not using an authenticator, you may also provide inline auth headers:
        # headers["Private-Token"] = self.config.get("refresh_token")
        return headers

    def get_next_page_token(
        self,
        response: requests.Response,
        previous_token: Any | None,
    ) -> Any | None:
        """Return a token for identifying next page or None if no more pages.

        Args:
            response: The HTTP ``requests.Response`` object.
            previous_token: The previous page token value.

        Returns:
            The next pagination token.
        """
        # TODO: If pagination is required, return a token which can be used to get the
        #       next page. If this is the final page, return "None" to end the
        #       pagination loop.
        if self.next_page_token_jsonpath:
            all_matches = extract_jsonpath(
                self.next_page_token_jsonpath, response.json()
            )
            first_match = next(iter(all_matches), None)
            next_page_token = first_match
        else:
            next_page_token = response.headers.get("X-Next-Page", None)

        return next_page_token

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

        print("\n\n ==== PARAM OUTPUT ===== \n\n")
        print("PATH: " + str(self.path))
        path = str(self.path)

        if str(self.path) == "adDirectSponsoredContents":
            params["q"] = "account"

        elif str(self.path) == "adAccounts" or str(self.path) == "adCampaigns" or str(self.path) == "adCampaignGroups":
            params["q"] = "search"
            params["sort.field"] = "ID"
            params["sort.order"] = "ASCENDING"

        elif str(self.path) == "adAccountUsers":
            params["q"] = "accounts"

        elif str(self.path) == "creatives":
            params["q"] = "criteria"
            params["campaigns"] = "List(urn%3Ali%3AsponsoredCampaign%3A{})"
            params["sortOrder"] = "ASCENDING"

        elif str(self.path) == "adAnalytics":
            params["q"] = "analytics"
            params["pivot"] = "CAMPAIGN"
            params["timeGranularity"] = "DAILY"
            params["dateRange.start.day"] = "24"
            params["dateRange.start.month"] = "2"
            params["dateRange.start.year"] = "2023"
            params["dateRange.end.day"] = "10"
            params["dateRange.end.month"] = "3"
            params["dateRange.end.year"] = "2023"
            params["campaigns[0]"] = "urn:li:sponsoredCampaign:211290954"



        print("THESE ARE THE PARAMS" + str(params))

        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:

        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        # TODO: Parse response body and return a set of records.
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary, or ``None`` to skip the record.
        """
        # TODO: Delete this method if not needed.
        return row