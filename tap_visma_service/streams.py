"""Stream type classes for tap-visma-service."""

from __future__ import annotations

import typing as t
from pathlib import Path
from datetime import datetime, timedelta
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

    # def get_new_paginator(self):
    #     # No pagination for this endpoint
    #     return None

    def get_url_params(self, context, next_page_token):
        # Get the base params from parent (pagination, start_date, replication_key)
        params = super().get_url_params(context, next_page_token)

        params.pop("pageNumber", None)

        # Add/override stream-specific params
        params.update({
            "includeAccountClassDescription": "true",
        })

        return params

class BranchesStream(VismaServiceStream):
    """Define custom stream."""

    name = "branches"
    path = "/v1/branch"
    primary_keys = ["branchId"]
    replication_key = "lastModifiedDateTime"
    schema_filepath = SCHEMAS_DIR / "branches.json"  # noqa: ERA001

    # def get_new_paginator(self):
    #     # No pagination for this endpoint
    #     return None

    def get_url_params(self, context, next_page_token):
        # Get the base params from parent (pagination, start_date, replication_key)
        params = super().get_url_params(context, next_page_token)

        params.pop("pageNumber", None)

        # Add/override stream-specific params
        params.update({
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
        })

        return params

    def get_child_context(self, record: dict, context: dict) -> dict:
        """Pass branchId to child stream"""
        return {"branchNumber": record["number"], "ledgerId": record["ledger"]["id"]}

# class BudgetsStream(VismaServiceStream):
#     """Define custom stream."""

#     name = "budgets"
#     path = "/v1/budget"
#     primary_keys = ["financialYear"]
#     replication_key = "lastModifiedDateTime"
#     schema_filepath = SCHEMAS_DIR / "budgets.json"  # noqa: ERA001
#     parent_stream_type = BranchesStream

#     def get_child_context(self, record, context):
#         return super().get_child_context(record, context)
    
#     # def get_new_paginator(self):
#     #     # No pagination for this endpoint
#     #     return None

#     def get_url_params(self, context, next_page_token):
#         # Get base params from parent (pagination, start_date, replication key)
#         params = super().get_url_params(context, next_page_token)

#         params.pop("pageNumber", None)

#         # Add stream-specific params (using context values)
#         params.update({
#             "branch": context["branchNumber"],
#             "ledger": context["ledgerId"],
#             "financialYear": "2023",
#         })

#         return params

class BudgetsStream(VismaServiceStream):
    """Define custom stream."""

    name = "budgets"
    path = "/v1/budget"
    primary_keys = ["financialYear", "branchNumber", "ledgerId"]  # Updated to include all dimensions
    replication_key = "lastModifiedDateTime"
    schema_filepath = SCHEMAS_DIR / "budgets.json"  # noqa: ERA001
    parent_stream_type = BranchesStream

    def get_child_context(self, record, context):
        return super().get_child_context(record, context)
    
    def get_financial_years(self):
        """Generate list of financial years from 2023 to current year."""
        current_year = datetime.today().year
        return list(range(2023, current_year + 1))
    
    def get_records(self, context):
        """Iterate over all ledgers and financial years for each branch and yield records."""
        # Get all ledgers
        ledgers_stream = LedgersStream(
            tap=self._tap,
            schema=self.schema,
            name="ledgers"
        )
        
        ledgers = list(ledgers_stream.get_records(context=None))
        financial_years = self.get_financial_years()
        
        # For each combination of ledger and financial year, fetch budgets
        for ledger in ledgers:
            ledger_id = ledger["number"]
            for financial_year in financial_years:
                self._current_ledger_id = ledger_id  # Store temporarily
                self._current_financial_year = str(financial_year)  # Store temporarily
                self.logger.info(
                    f"Fetching budgets for branch {context.get('branchNumber')}, "
                    f"ledger {ledger_id}, financial year {financial_year}..."
                )
                yield from super().get_records(context)
        
        self._current_ledger_id = None
        self._current_financial_year = None

    def get_url_params(self, context, next_page_token):
        # Get base params from parent (pagination, start_date, replication key)
        params = super().get_url_params(context, next_page_token)

        params.pop("pageNumber", None)

        # Get the current ledger ID and financial year
        ledger_id = getattr(self, "_current_ledger_id", None)
        financial_year = getattr(self, "_current_financial_year", None)
        
        if ledger_id is None:
            # Fallback to context if available
            ledger_id = context.get("ledgerId")
        
        if financial_year is None:
            # Fallback to default
            financial_year = "2023"

        # Add stream-specific params (using context values)
        params.update({
            "branch": context["branchNumber"],
            "ledger": ledger_id,
            "financialYear": financial_year,
        })

        return params

class DepartmentsStream(VismaServiceStream):
    """Define custom stream."""

    name = "departments"
    path = "/v1/department"
    primary_keys = ["departmentId"]
    replication_key = "lastModifiedDateTime"
    schema_filepath = SCHEMAS_DIR / "departments.json"  # noqa: ERA001

    # def get_new_paginator(self):
    #     # No pagination for this endpoint
    #     return None

    # def get_url_params(self, context, next_page_token):
    #     params = super().get_url_params(context, next_page_token)
    #     params.pop("pageNumber", None)
    #     return params

class LedgersStream(VismaServiceStream):
    """Define custom stream."""

    name = "ledgers"
    path = "/v1/ledger"
    primary_keys = ["internalId"]
    replication_key = "lastModifiedDateTime"
    schema_filepath = SCHEMAS_DIR / "ledgers.json"  # noqa: ERA001

    def get_child_context(self, record: dict, context: dict) -> dict:
        """Pass branchId to child stream"""
        return {"ledgerId": record["number"]}
    

class GeneralLedgerTransactionsStream(VismaServiceStream):
    """Define custom stream."""

    name = "general_ledger_transactions"
    path = "/v1/GeneralLedgerTransactions"
    primary_keys = ["lineNumber", "batchNumber"]
    replication_key = "lastModifiedDateTime"
    schema_filepath = SCHEMAS_DIR / "general_ledger_transactions.json"  # noqa: ERA001
    parent_stream_type = LedgersStream

    def get_child_context(self, record, context):
        return super().get_child_context(record, context)

    def get_url_params(self, context, next_page_token):
        # Get base params from parent (pagination, start_date, replication key)
        params = super().get_url_params(context, next_page_token)

        # Get start_date from config or fallback
        start_date_str = self.config.get("start_date", "2023-01-01")
        start_date = datetime.fromisoformat(start_date_str.replace("Z", "").replace("T", " "))
        from_period = start_date.strftime("%Y%m")

        # Today's period in YYYYMM
        to_period = datetime.today().strftime("%Y%m")

        # Add stream-specific params
        params.update({
            "ledger": context["ledgerId"],
            "FromPeriod": from_period,
            "ToPeriod": to_period,
            "expandAccountInfo": "true",
            "expandBranchInfo": "true",
            "includeTransactionBalance": "true",
        })

        return params


class JournalTransactionsStream(VismaServiceStream):
    """Define custom stream."""

    name = "journal_transactions"
    path = "/v2/journaltransaction"
    # primary_keys = ["journalTransactionId"]
    replication_key = "lastModifiedDateTime"
    schema_filepath = SCHEMAS_DIR / "journal_transactions.json"  # noqa: ERA001

    # def get_url_params(self, context, next_page_token):
    #     # Get base params (pagination, start_date, replication key)
    #     params = super().get_url_params(context, next_page_token)

    #     params.pop("lastModifiedDateTime", None)
    #     params.pop("lastModifiedDateTimeCondition", None)

    #     if self.config.get("start_date"):
    #         # Use LastModifiedDateTime if start_date is provided
    #         last_modified = self.config["start_date"]
    #         params.update({
    #             "LastModifiedDateTime": last_modified,
    #         })
    #     else:
    #         # Use PeriodId in YYYYMM format
    #         start_period = "2023-01-01"
    #         params.update({
    #             "LastModifiedDateTime": start_period,
    #         })

    #     return params

    def get_period_list(self):
        """Generate all YYYYMM period IDs from start_date up to today."""
        if self.config.get("start_date"):
            start_date = datetime.fromisoformat(
                self.config["start_date"].replace("Z", "").replace("T", " ")
            )
        else:
            start_date = datetime(2023, 1, 1)

        end_date = datetime.today()
        periods = []

        current = start_date
        while current <= end_date:
            periods.append(current.strftime("%Y%m"))
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)

        return periods

    def get_records(self, context):
        """Iterate over all periodIds and yield records."""
        for period_id in self.get_period_list():
            self._current_period_id = period_id  # temporarily store it
            self.logger.info(f"Fetching records for period {period_id}...")
            yield from super().get_records(context)
        self._current_period_id = None

    def get_url_params(self, context, next_page_token):
        """Provide URL params including periodId."""
        params = super().get_url_params(context, next_page_token)

        params.pop("lastModifiedDateTime", None)
        params.pop("lastModifiedDateTimeCondition", None)

        period_id = getattr(self, "_current_period_id", None)
        if period_id is None:
            if self.config.get("start_date"):
                start_date = datetime.fromisoformat(
                    self.config["start_date"].replace("Z", "").replace("T", " ")
                )
                period_id = start_date.strftime("%Y%m")
            else:
                period_id = "202301"

        params["periodId"] = period_id
        return params

    # def get_url_params(self, context, next_page_token):
    #     # Get base params from parent (pagination, pageNumber, etc.)
    #     params = super().get_url_params(context, next_page_token)

    #     # Remove any leftover lastModified keys
    #     params.pop("lastModifiedDateTime", None)
    #     params.pop("lastModifiedDateTimeCondition", None)

    #     # Compute PeriodId
    #     if self.config.get("start_date"):
    #         start_date_str = self.config["start_date"]
    #         start_date = datetime.fromisoformat(start_date_str.replace("Z", "").replace("T", " "))
    #         period_id = start_date.strftime("%Y%m")
    #     else:
    #         # Default period
    #         period_id = "202301"

    #     params.update({
    #         "periodId": period_id
    #     })

    #     return params

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

class SubaccountsStream(VismaServiceStream):
    """Define custom stream."""

    name = "subaccounts"
    path = "/v1/subaccount"
    primary_keys = ["subaccountId"]
    replication_key = "lastModifiedDateTime"
    schema_filepath = SCHEMAS_DIR / "subaccounts.json"

class SuppliersStream(VismaServiceStream):
    """Define custom stream."""

    name = "suppliers"
    path = "/v1/supplier"
    primary_keys = ["internalId"]
    replication_key = "lastModifiedDateTime"
    schema_filepath = SCHEMAS_DIR / "suppliers.json"
