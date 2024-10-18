"""REST client handling, including LinkedInAdsStream base class."""

from __future__ import annotations

import contextlib
import typing as t
from datetime import datetime, timezone
from pathlib import Path

import requests
from singer_sdk.authenticators import (
    BearerTokenAuthenticator,
    OAuthAuthenticator,
    SingletonMeta,
)
from singer_sdk.streams import RESTStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
UTC = timezone.utc

_Auth = t.Callable[[requests.PreparedRequest], requests.PreparedRequest]


class LinkedInAdsOAuthAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """Authenticator class for LinkedInAds."""

    @property
    def oauth_request_body(self):
        return {
            "grant_type": "refresh_token",
            "client_id": self.config["oauth_credentials"]["client_id"],
            "client_secret": self.config["oauth_credentials"]["client_secret"],
            "refresh_token": self.config["oauth_credentials"]["refresh_token"],
        }


class LinkedInAdsStream(RESTStream):
    """LinkedInAds stream class."""

    records_jsonpath = "$[*]"  # Or override `parse_response`.
    next_page_token_jsonpath = (
        "$.paging.start"  # Or override `get_next_page_token`.  # noqa: S105
    )

    @property
    def authenticator(self) -> _Auth:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        if "oauth_credentials" in self.config:
            return LinkedInAdsOAuthAuthenticator(
                self,
                auth_endpoint="https://www.linkedin.com/oauth/v2/accessToken",
            )
        return BearerTokenAuthenticator.create_for_stream(
            self,
            token=self.config["access_token"],
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config["user_agent"]
        headers["LinkedIn-Version"] = "202305"
        headers["Content-Type"] = "application/json"
        headers["X-Restli-Protocol-Version"] = "1.0.0"

        return headers

    def get_next_page_token(
        self,
        response: requests.Response,
        previous_token: t.Any | None,  # noqa: ANN401
    ) -> t.Any | None:  # noqa: ANN401
        """Return a token for identifying next page or None if no more pages."""
        # If pagination is required, return a token which can be used to get the
        #       next page. If this is the final page, return "None" to end the
        #       pagination loop.
        resp_json = response.json()
        if previous_token is None:
            previous_token = 0

        elements = resp_json.get("elements")

        if elements is None:
            page = resp_json
            if len(page) in [0, previous_token + 1]:
                return None

        elif len(elements) in [0, previous_token + 1]:
            return None
        return previous_token + 1

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
        params: dict = {}
        if next_page_token:
            params["start"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key

        return params

    def parse_response(
        self,
        response: requests.Response,
    ) -> t.Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        resp_json = response.json()
        if resp_json.get("elements") is not None:
            results = resp_json["elements"]
            try:
                columns = results[0]
            except Exception:  # noqa: BLE001
                columns = results
            with contextlib.suppress(Exception):
                self._add_datetime_columns(columns)

        else:
            results = resp_json
            try:
                columns = results
                self._add_datetime_columns(columns)
            except Exception:  # noqa: BLE001
                columns = results
        with contextlib.suppress(Exception):
            self._to_id_column(columns, "account", "account_id")

        with contextlib.suppress(Exception):
            self._to_id_column(
                columns,
                "campaignGroup",
                "campaign_group_id",
            )
        with contextlib.suppress(Exception):
            schedule_column = columns.get("runSchedule").get("start")
            columns["run_schedule_start"] = datetime.fromtimestamp(  # noqa: DTZ006
                int(schedule_column) / 1000,
            ).isoformat()
        yield from (
            resp_json["elements"]
            if resp_json.get("elements") is not None
            else [columns]
        )

    def _to_id_column(
        self,
        columns,  # noqa: ANN001
        arg1,  # noqa: ANN001
        arg2,  # noqa: ANN001
    ) -> None:
        account_column = columns.get(arg1)
        account_id = int(account_column.split(":")[3])
        columns[arg2] = account_id

    def _add_datetime_columns(self, columns):  # noqa: ANN202, ANN001
        created_time = columns.get("changeAuditStamps").get("created").get("time")
        last_modified_time = (
            columns.get("changeAuditStamps").get("lastModified").get("time")
        )
        columns["created_time"] = datetime.fromtimestamp(
            int(created_time) / 1000,
            tz=UTC,
        ).isoformat()
        columns["last_modified_time"] = datetime.fromtimestamp(
            int(last_modified_time) / 1000,
            tz=UTC,
        ).isoformat()
