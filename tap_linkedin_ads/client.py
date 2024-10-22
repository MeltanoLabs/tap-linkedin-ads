"""REST client handling, including LinkedInAdsStream base class."""

from __future__ import annotations

import typing as t
from functools import cached_property
from importlib import resources

from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator  # noqa: TCH002
from singer_sdk.streams import RESTStream
from singer_sdk.pagination import BaseAPIPaginator  # noqa: TCH002

from tap_linkedin_ads.auth import LinkedInAdsOAuthAuthenticator

if t.TYPE_CHECKING:
    import requests
    from singer_sdk.helpers.types import Auth, Context


# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = resources.files(__package__) / "schemas"


class LinkedInAdsStream(RESTStream):
    """LinkedInAds stream class."""

    # Update this value if necessary or override `parse_response`.
    records_jsonpath = "$.elements[*]"

    # Update this value if necessary or override `get_new_paginator`.
    next_page_token_jsonpath = "$.metadata.nextPageToken"  # noqa: S105

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return "https://api.linkedin.com/rest"

    @cached_property
    def authenticator(self) -> Auth:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        if "oauth_credentials" in self.config:
            return LinkedInAdsOAuthAuthenticator.create_for_stream(self)
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
        headers["LinkedIn-Version"] = "202404"
        headers["Content-Type"] = "application/json"
        headers["X-Restli-Protocol-Version"] = "2.0.0"

        return headers

    def get_new_paginator(self) -> BaseAPIPaginator:
        """Get the paginator."""
        return super().get_new_paginator()

    def get_url_params(
        self,
        context: Context | None,  # noqa: ARG002
        next_page_token: t.Any | None,  # noqa: ANN401
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {
            "q": "search",
        }
        if next_page_token:
            params["pageToken"] = next_page_token
        # if self.replication_key:
        #     params["sort"] = "asc"
        #     params["order_by"] = self.replication_key
        return params

    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())
