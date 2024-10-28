"""LinkedInAds Authentication."""

from __future__ import annotations

from singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta


# The SingletonMeta metaclass makes your streams reuse the same authenticator instance.
# If this behaviour interferes with your use-case, you can remove the metaclass.
class LinkedInAdsOAuthAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """Authenticator class for LinkedInAds."""

    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the AutomaticTestTap API.

        Returns:
            A dict with the request body
        """
        return {
            "grant_type": "refresh_token",
            "client_id": self.config["oauth_credentials"]["client_id"],
            "client_secret": self.config["oauth_credentials"]["client_secret"],
            "refresh_token": self.config["oauth_credentials"]["refresh_token"],
        }

    @classmethod
    def create_for_stream(cls, stream) -> LinkedInAdsOAuthAuthenticator:  # noqa: ANN001
        """Instantiate an authenticator for a specific Singer stream.

        Args:
            stream: The Singer stream instance.

        Returns:
            A new authenticator.
        """
        return cls(
            stream=stream,
            auth_endpoint="https://www.linkedin.com/oauth/v2/accessToken",
        )
