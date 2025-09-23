"""Stream type classes for tap-visma-service."""

from __future__ import annotations

import typing as t
from pathlib import Path

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_visma_service.client import VismaServiceStream

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


class AccountsStream(VismaServiceStream):
    """Define custom stream."""

    name = "accounts"
    path = "/v1/account"
    primary_keys = ["accountID"]
    replication_key = "lastModifiedDateTime"
    schema_filepath = SCHEMAS_DIR / "accounts.json"  # noqa: ERA001

class BranchesStream(VismaServiceStream):
    """Define custom stream."""

    name = "branches"
    path = "/v1/branch"
    primary_keys = ["branchId"]
    replication_key = "lastModifiedDateTime"
    schema_filepath = SCHEMAS_DIR / "branches.json"  # noqa: ERA001

    def get_url_params(self, context, next_page_token):

        params = {
            "expandAddress": "true",
            "expandContact": "true",
            "expandCurrency": "true",
            "expandVatZone": "true",
            "expandLedger": "true",
            "expandIndustryCode": "true",
            "expandDeliveryAddress": "true",
            "expandDeliveryContact": "true",
            "expandDefaultCountry": "true",
            "expandBankSettings": "true",
        }
        return params

    def get_child_context(self, record: dict, context: dict) -> dict:
        """Pass branchId to child stream"""
        return {"branchNumber": record["number"], "ledgerId": record["ledger"]["id"]}

class BudgetsStream(VismaServiceStream):
    """Define custom stream."""

    name = "budgets"
    path = "/v1/budget"
    primary_keys = ["financialYear"]
    replication_key = "lastModifiedDateTime"
    schema_filepath = SCHEMAS_DIR / "budgets.json"  # noqa: ERA001
    parent_stream_type = BranchesStream

    def get_child_context(self, record, context):
        return super().get_child_context(record, context)

    def get_url_params(self, context, next_page_token):

        params = {
            "branch": context["branchNumber"],
            "ledger": context["ledgerId"],
            "financialYear": "2023",
        }
        return params

class DepartmentsStream(VismaServiceStream):
    """Define custom stream."""

    name = "departments"
    path = "/v1/department"
    primary_keys = ["departmentId"]
    replication_key = "lastModifiedDateTime"
    schema_filepath = SCHEMAS_DIR / "departments.json"  # noqa: ERA001

class GeneralLedgerTransactionsStream(VismaServiceStream):
    """Define custom stream."""

    name = "general_ledger_transactions"
    path = "/v1/GeneralLedgerTransactions"
    primary_keys = ["lineNumber", "batchNumber"]
    replication_key = "lastModifiedDateTime"
    schema_filepath = SCHEMAS_DIR / "general_ledger_transactions.json"  # noqa: ERA001
    parent_stream_type = BranchesStream

    def get_child_context(self, record, context):
        return super().get_child_context(record, context)

    def get_url_params(self, context, next_page_token):

        params = {
            "ledger": context["ledgerId"],
            "lastModifiedDateTime": "2025-09-15",
        }
        return params

class JournalTransactionsStream(VismaServiceStream):
    """Define custom stream."""

    name = "journal_transactions"
    path = "/v2/journaltransaction"
    # primary_keys = ["journalTransactionId"]
    replication_key = "lastModifiedDateTime"
    schema_filepath = SCHEMAS_DIR / "journal_transactions.json"  # noqa: ERA001

    def get_url_params(self, context, next_page_token):

        params = {
            "lastModifiedDateTime": "2025-09-15",
        }
        return params

class LedgersStream(VismaServiceStream):
    """Define custom stream."""

    name = "ledgers"
    path = "/v1/ledger"
    primary_keys = ["internalId"]
    replication_key = "lastModifiedDateTime"
    schema_filepath = SCHEMAS_DIR / "ledgers.json"  # noqa: ERA001

class ProjectsStream(VismaServiceStream):
    """Define custom stream."""

    name = "projects"
    path = "/v1/project"
    primary_keys = ["projectID"]
    replication_key = "lastModifiedDateTime"
    schema_filepath = SCHEMAS_DIR / "projects.json"  # noqa: ERA001

class ProjectAccountGroupsStream(VismaServiceStream):
    """Define custom stream."""

    name = "project_account_groups"
    path = "/v1/projectaccountgroup"
    primary_keys = ["accountGroupId"]
    replication_key = "accountGroupId"
    schema_filepath = SCHEMAS_DIR / "project_account_groups.json"  # noqa: ERA001

class ProjectBudgetsStream(VismaServiceStream):
    """Define custom stream."""

    name = "project_budgets"
    path = "/v1/projectbudget"
    primary_keys = ["projectID"]
    replication_key = "projectID"
    schema_filepath = SCHEMAS_DIR / "project_budgets.json"  # noqa: ERA001



