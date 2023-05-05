"""REST client handling, including LinkedInStream base class."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Iterable

import requests
from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream

from datetime import datetime


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
            headers["X-Restli-Protocol-Version"] = self.config.get(
                "x-restli-protocol-version"
            )
            headers["Content-Type"] = self.config.get("application/json")

        # If not using an authenticator, you may also provide inline auth headers:
        # headers["Private-Token"] = self.config.get("refresh_token")

        return headers

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        # If pagination is required, return a token which can be used to get the
        #       next page. If this is the final page, return "None" to end the
        #       pagination loop.

        resp_json = response.json()
        if previous_token == None:
            previous_token = 0

        if len(resp_json.get("elements")) == 0:
            next_page_token = None
        elif len(resp_json.get("elements")) == previous_token:
            next_page_token = None
        else:
            next_page_token = previous_token + 1

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

        return params


    adanalytics_columns_first = {}
    adanalytics_columns_second = {}


    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """

        resp_json = response.json()

        if isinstance(resp_json, list):
            results = resp_json
        elif resp_json.get("elements") is not None:
            results = resp_json["elements"]
            try:
                columns = results[0]
                created_time = (
                    columns.get("changeAuditStamps").get("created").get("time")
                )
                last_modified_time = (
                    columns.get("changeAuditStamps").get("lastModified").get("time")
                )
                columns["created_time"] = datetime.fromtimestamp(
                    int(created_time) / 1000
                ).isoformat()
                columns["last_modified_time"] = datetime.fromtimestamp(
                    int(last_modified_time) / 1000
                ).isoformat()
                try:
                    account_column = columns.get("account")
                    account_id = int(account_column.split(":")[3])
                    columns["account_id"] = account_id
                except:
                    pass
                try:
                    campaign_column = columns.get("campaignGroup")
                    campaign = int(campaign_column.split(":")[3])
                    columns["campaign_group_id"] = campaign
                except:
                    pass
                try:
                    user_column = columns.get("user")
                    user = user_column.split(":")[3]
                    columns["user_person_id"] = user
                except:
                    pass
                try:
                    schedule_column = columns.get("runSchedule").get("start")
                    columns["run_schedule_start"] = datetime.fromtimestamp(
                        int(schedule_column) / 1000
                    ).isoformat()
                except:
                    pass
                results = [columns]
            except:
                pass
        else:
            results = resp_json

        yield from results
