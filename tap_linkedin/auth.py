"""linkedin Authentication."""


from singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta


# The SingletonMeta metaclass makes your streams reuse the same authenticator instance.
# If this behaviour interferes with your use-case, you can remove the metaclass.
class linkedinAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """Authenticator class for linkedin."""

    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the linkedin API."""
        # TODO: Define the request body needed for the API.
        return {
            'resource': 'https://analysis.windows.net/powerbi/api',
            'scope': self.oauth_scopes,
            'client_id': self.config["client_id"],
            'client_secret': self.config["client_secret"],
            'grant_type': 'Authorization Code',
        }

    @classmethod
    def create_for_stream(cls, stream) -> "linkedinAuthenticator":
        return cls(
            stream=stream,
            auth_endpoint="https://www.linkedin.com/oauth/v2/authorization",
            oauth_scopes="r_ads,r_ads_reporting,r_basicprofile",
        )
