import csv
import dataclasses
import enum
import os
import re
import sys
from decimal import Decimal
from typing import Dict, List, Tuple

import arrow
import requests
import toml

YNAB_FIELDS = (
    "Account,Flag,Check Number,Date,Payee,Category,Master Category,Sub Category,Memo,Outflow,Inflow,Cleared,"
    "Running Balance".split(",")
)


@dataclasses.dataclass
class Config:
    # Mapping of Account Name to foreign currency code (ISO 4217)
    foreign_accounts: Dict[str, str] = dataclasses.field(default_factory=dict)

    # Mapping of currency conversion fallback if foreign amount can't be automatically determined
    currency_conv_fallback: Dict[str, Decimal] = dataclasses.field(default_factory=dict)

    # Mapping of Account Name to account roles (credit_card / cash / savings)
    accounts: Dict[str, str] = dataclasses.field(default_factory=dict)

    date_format: str = "MM/DD/YYYY"

    # Mapping for renaming payees
    payee_mapping: Dict[str, str] = dataclasses.field(default_factory=dict)

    # Which YNAB field to use for category?
    # Category: Everyday Expenses:Household
    # Master Category: Everyday Expenses
    # Sub Category: Household
    category_field: str = "Category"

    # Which YNAB field to use for budget?
    # Category: Everyday Expenses:Household
    # Master Category: Everyday Expenses
    # Sub Category: Household
    budget_field: str = "Sub Category"

    # Should memo field be used to fill in description? It will be moved to Notes otherwise
    memo_to_description: bool = True

    # What value should be used if a description can't be determined automatically?
    # This is also the value that'll get used if `memo_to_description` is false.
    empty_description: str = "(empty description)"


@dataclasses.dataclass
class ImportData:
    @dataclasses.dataclass
    class Account:
        class Role(enum.Enum):
            credit_card = "ccAsset"
            default = "defaultAsset"
            savings = "savingAsset"
            cash = "cashWalletAsset"

        name: str
        starting_date: arrow.Arrow
        account_role: Role = Role.default
        opening_balance: Decimal = Decimal(0)
        currency_code: str = "USD"

    @dataclasses.dataclass
    class Withdrawal:
        account: str
        date: arrow.Arrow
        payee: str
        amount: Decimal
        description: str
        budget: str = ""
        category: str = ""
        notes: str = ""

    @dataclasses.dataclass
    class Deposit:
        account: str
        date: arrow.Arrow
        payee: str
        amount: Decimal
        description: str
        budget: str = ""
        category: str = ""
        notes: str = ""

    @dataclasses.dataclass
    class Transfer:
        from_account: str
        to_account: str
        date: arrow.Arrow
        amount: Decimal
        description: str
        # set if exactly one of: from, to is foreign currency account
        foreign_amount: Decimal = None
        notes: str = ""

    asset_accounts: List[Account] = dataclasses.field(default_factory=list)
    revenue_accounts: List[str] = dataclasses.field(default_factory=list)
    expense_accounts: List[str] = dataclasses.field(default_factory=list)

    withdrawals: List[Withdrawal] = dataclasses.field(default_factory=list)
    deposits: List[Deposit] = dataclasses.field(default_factory=list)
    transfers: List[Transfer] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class FireflyData:
    asset_accounts: Dict[str, int] = dataclasses.field(default_factory=dict)
    revenue_accounts: Dict[str, int] = dataclasses.field(default_factory=dict)
    expense_accounts: Dict[str, int] = dataclasses.field(default_factory=dict)


def create_transaction():
    pass


AMOUNT_RE = re.compile(r"^.*([0-9,\\.]+)$")


def _to_amount(s: str) -> Decimal:
    m = AMOUNT_RE.match(s)
    assert m, f"Invalid value with no amount: |{s}|"
    return Decimal(m.group(1).replace(",", ""))


def _is_transfer(tx: Dict) -> bool:
    return "Transfer : " in tx["Payee"]


class Importer:
    MEMO_RE = re.compile(r".*([A-Z]{3})\s+([0-9,\\.]+);?.*")

    def __init__(self, config_path, register_path):
        self.firefly_url = os.environ["FIREFLY_III_URL"]
        self.firefly_token = os.environ["FIREFLY_III_ACCESS_TOKEN"]

        self.config = Config(**toml.load(config_path))
        print(f"Loaded config for import into {self.firefly_url}")

        self.register_path = register_path

        self.data = ImportData()
        self.firefly_date = FireflyData()

    def _parse_date(self, dt: str) -> arrow.Arrow:
        return arrow.get(dt, self.config.date_format)

    def _date(self, tx: Dict) -> arrow.Arrow:
        return self._parse_date(tx["Date"])

    def _payee(self, tx: Dict) -> str:
        return self.config.payee_mapping.get(tx["Payee"], tx["Payee"])

    def _foreign_amount_from_memo(self, tx: Dict) -> Decimal:
        # for foreign accounts, try to use memo to find real value at the time of transaction
        m = self.MEMO_RE.match(tx["Memo"])
        foreign_currency_code = self.config.foreign_accounts[tx["Account"]]
        if m and m.group(1) != self.config.foreign_accounts[tx["Account"]]:
            # use memo - yaay!
            return Decimal(m.group(2)) * (-1 if _to_amount(tx["Inflow"]) > 0 else 1)

        conv_fallback = self.config.currency_conv_fallback.get(foreign_currency_code)
        if not conv_fallback:
            raise ValueError(
                f"Unable to determine foreign amount for {tx}. Memo must be of form: [CURRENCY CODE] "
                f"[AMOUNT]. Alternatively, set config.currency_conv_fallback."
            )
        return amount * conv_fallback

    def _amount(self, tx: Dict) -> Decimal:
        # exactly one of these would be non-zero
        amount = _to_amount(tx["Outflow"]) - _to_amount(tx["Inflow"])
        if tx["Account"] not in self.config.foreign_accounts:
            return amount

        return self._foreign_amount_from_memo(tx)

    def _category(self, tx: Dict) -> str:
        return tx[self.config.category_field]

    def _budget(self, tx: Dict) -> str:
        return tx[self.config.budget_field]

    def _description(self, tx: Dict) -> str:
        if not self.config.memo_to_description:
            return self.config.empty_description
        if "(Split" in tx["Memo"]:
            return tx["Memo"].split(") ")[1].strip() or self.config.empty_description
        return tx["Memo"].strip() or self.config.empty_description

    def _notes(self, tx: Dict) -> str:
        if not self.config.memo_to_description:
            return tx["Memo"].strip()
        return ""

    def _transfer_account(self, tx: Dict) -> str:
        if " / Transfer : " in tx["Payee"]:
            return tx["Payee"].split(" / ")[1].split(" : ")[1]
        return tx["Payee"].split(" : ")[1]

    def _transfer_foreign_amount(self, tx: Dict) -> Decimal:
        to_account = self._transfer_account(tx)
        # From foreign account to default currency account --
        if tx["Account"] in self.config.foreign_accounts and to_account not in self.config.foreign_accounts:
            # Amount is already computed in `self._amount` above using foreign currency code
            # Outflow / Inflow corresponds to default currency
            return _to_amount(tx["Outflow"]) - _to_amount(tx["Inflow"])
        # From default currency account to foreign account
        elif tx["Account"] not in self.config.foreign_accounts and to_account in self.config.foreign_accounts:
            # Amount is default currency
            return self._foreign_amount_from_memo(tx)
        elif tx["Account"] in self.config.foreign_accounts and to_account in self.config.foreign_accounts:
            assert (
                self.config.foreign_accounts[tx["Account"]] == self.config.foreign_accounts[to_account]
            ), f"Can't handle transaction between two different foreign accounts: {tx}"

    def run(self, dry_run: bool = False):
        with open(self.register_path) as f:
            reader = csv.DictReader(f, fieldnames=YNAB_FIELDS)
            # skip header
            next(reader)
            all_transactions = list(reader)
        print(f"Loaded {len(all_transactions)} transactions")

        account_names = {tx["Account"] for tx in all_transactions}
        for acc in self.config.foreign_accounts:
            assert acc in account_names, f"Invalid foreign account in config: |{acc}|"

        starting_balances: Dict[str, Tuple[str, Decimal]] = {
            tx["Account"]: (tx["Date"], _to_amount(tx["Inflow"]) - _to_amount(tx["Outflow"]),)
            for tx in all_transactions
            if tx["Payee"] == "Starting Balance"
        }

        for acc in account_names:
            start_date, balance = starting_balances[acc]
            account = ImportData.Account(
                name=acc,
                starting_date=self._parse_date(start_date),
                account_role=ImportData.Account.Role[self.config.accounts.get(acc, "default")],
                opening_balance=balance,
            )
            self.data.asset_accounts.append(account)

        self.data.revenue_accounts = list(
            {self._payee(tx) for tx in all_transactions if _to_amount(tx["Inflow"]) > 0 and not _is_transfer(tx)}
        )
        self.data.expense_accounts = list(
            {self._payee(tx) for tx in all_transactions if _to_amount(tx["Outflow"]) > 0 and not _is_transfer(tx)}
        )
        print(
            f"Configured account data for {len(self.data.asset_accounts)} asset accounts, "
            f"{len(self.data.revenue_accounts)} revenue accounts, and {len(self.data.expense_accounts)} "
            f"expense accounts"
        )

        for tx in all_transactions:
            if _to_amount(tx["Outflow"]) > 0:
                withdrawal = ImportData.Withdrawal(
                    account=tx["Account"],
                    date=self._date(tx),
                    payee=self._payee(tx),
                    amount=self._amount(tx),
                    description=self._description(tx),
                    budget=self._budget(tx),
                    category=self._category(tx),
                    notes=self._notes(tx),
                )
                self.data.withdrawals.append(withdrawal)
            elif _to_amount(tx["Inflow"]) > 0:
                deposit = ImportData.Deposit(
                    account=tx["Account"],
                    date=self._date(tx),
                    payee=self._payee(tx),
                    amount=self._amount(tx),
                    description=self._description(tx),
                    budget=self._budget(tx),
                    category=self._category(tx),
                    notes=self._notes(tx),
                )
                self.data.deposits.append(deposit)
            elif _is_transfer(tx):
                transfer = ImportData.Transfer(
                    from_account=tx["Account"],
                    to_account=self._transfer_account(tx),
                    date=self._date(tx),
                    amount=self._amount(tx),
                    description=self._description(tx),
                    foreign_amount=self._transfer_foreign_amount(tx),
                    notes=self._notes(tx),
                )
        print(f"Configured {len(self.data.deposits)} deposits and {len(self.data.withdrawals)} withdrawals")

        self._create_asset_accounts()
        self._create_revenue_accounts()
        self._create_expense_accounts()

    def _create_asset_accounts(self) -> None:
        pass

    def _create_revenue_accounts(self) -> None:
        pass

    def _create_expense_accounts(self) -> None:
        pass


if __name__ == "__main__":
    importer = Importer(sys.argv[1], sys.argv[2])
    importer.run()
