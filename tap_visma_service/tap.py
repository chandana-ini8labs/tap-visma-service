"""VismaService tap class."""

from __future__ import annotations

import sys

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_visma_service import streams

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


class TapVismaService(Tap):
    """VismaService tap class."""

    name = "tap-visma-service"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "client_id",
            th.StringType(nullable=False),
            required=True,
            title="Client ID",
            description="The client ID to authenticate against the API service",
        ),
        th.Property(
            "client_secret",
            th.StringType(nullable=False),
            required=True,
            secret=True,  # Flag config as protected.
            title="Client Secret",
            description="The client secret to authenticate against the API service",
        ),
        th.Property(
            "tenant_id",
            th.StringType(nullable=False),
            title="Tenant ID",
            description="The tenant ID to authenticate against the API service",
        ),
        th.Property(
            "start_date",
            th.DateTimeType(nullable=True),
            description="The earliest record date to sync",
        )
    ).to_dict()

    @override
    def discover_streams(self) -> list[streams.VismaServiceStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.AccountsStream(self),
            streams.BranchesStream(self),
            streams.BudgetsStream(self),
            streams.DepartmentsStream(self),
            streams.GeneralLedgerTransactionsStream(self),
            streams.JournalTransactionsStream(self),
            streams.LedgersStream(self),
            streams.ProjectsStream(self),
            streams.ProjectAccountGroupsStream(self),
            streams.ProjectBudgetsStream(self),
            streams.SubaccountsStream(self),
            streams.SuppliersStream(self),
        ]


if __name__ == "__main__":
    TapVismaService.cli()
