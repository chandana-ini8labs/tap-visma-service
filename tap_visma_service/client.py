"""REST client handling, including VismaServiceStream base class."""

from __future__ import annotations

import decimal
import sys
import typing as t
from functools import cached_property
from importlib import resources

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator  # noqa: TC002
from singer_sdk.streams import RESTStream
from typing import Any, Dict, Optional, cast, Iterable

from tap_visma_service.auth import VismaServiceAuthenticator

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    import requests
    from singer_sdk.helpers.types import Auth, Context


# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = resources.files(__package__) / "schemas"

    
class PageNumberPaginator(BaseAPIPaginator[int]):
    """Paginator for Visma APIs using `pageNumber` parameter."""

    def __init__(self, start_value: int = 1, page_size: int = 1000) -> None:
        super().__init__(start_value=start_value)
        self.page_size = page_size  # max records per page

    def get_next(self, response: Any) -> int | None:
        """Return the next page number or None if no more data."""
        data = response.json()

        # Stop if empty list
        if not data:
            return None

        # Stop if fewer items than page_size (last page)
        if len(data) < self.page_size:
            return None

        # Otherwise, go to next page
        return self.current_value + 1


class VismaServiceStream(RESTStream):
    """VismaService stream class."""

    # Update this value if necessary or override `parse_response`.
    records_jsonpath = "$[*]"

    # # Update this value if necessary or override `get_new_paginator`.
    # next_page_token_jsonpath = "$.next_page"  # noqa: S105

    @override
    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        # TODO: hardcode a value here, or retrieve it from self.config
        return "https://api.finance.visma.net"

    @override
    @cached_property
    def authenticator(self) -> Auth:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return VismaServiceAuthenticator(
            client_id=self.config["client_id"],
            client_secret=self.config["client_secret"],
            auth_endpoint="https://connect.visma.com/connect/token",
            oauth_scopes="vismanet_erp_service_api:read",
        )

    @property
    @override
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        return {}

    def get_new_paginator(self) -> BaseAPIPaginator | None:
        return PageNumberPaginator(start_value=1)

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {}

        # Pagination
        params["pageNumber"] = next_page_token or 1

        # Replication / incremental key
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key

        # Start date filter (applies to all streams)
        if self.config.get("start_date"):
            params["lastModifiedDateTime"] = self.config["start_date"]
            params["lastModifiedDateTimeCondition"] = "%3E%3D"

        return params
    

    @override
    def prepare_request_payload(
        self,
        context: Context | None,
        next_page_token: t.Any | None,
    ) -> dict | None:
        """Prepare the data payload for the REST API request.

        By default, no payload will be sent (return None).

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary with the JSON body for a POST requests.
        """
        # TODO: Delete this method if no payload is required. (Most REST APIs.)
        return None

    @override
    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        # TODO: Parse response body and return a set of records.
        yield from extract_jsonpath(
            self.records_jsonpath,
            input=response.json(parse_float=decimal.Decimal),
        )

    @override
    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Note: As of SDK v0.47.0, this method is automatically executed for all stream types.
        You should not need to call this method directly in custom `get_records` implementations.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary, or ``None`` to skip the record.
        """
        # TODO: Delete this method if not needed.
        return row
