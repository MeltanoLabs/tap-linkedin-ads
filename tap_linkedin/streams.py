"""Stream type classes for tap-linkedin-sdk."""

from __future__ import annotations

from pathlib import Path

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_linkedin.client import LinkedInStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class Accounts(LinkedInStream):
    """
    https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-accounts#search-for-accounts
    """
    name = "accounts"
    schema_filepath = "tap_linkedin/schemas/accounts.json"
    tap_stream_id = "accounts"
    replication_method = "INCREMENTAL"
    replication_keys = ["last_modified_time"]
    key_properties = ["id"]
    account_filter = "search_id_values_param"
    path = "adAccounts"
    data_key = "elements"
    children = ["video_ads"]
    params = {
        "q": "search",
        "sort.field": "ID",
        "sort.order": "ASCENDING"
    }