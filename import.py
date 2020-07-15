import csv
import dataclasses
import enum
import json
import os
import re
import sys
from collections import defaultdict
from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Union

import arrow
import dacite
import funcy
import requests
import toml

YNAB_FIELDS = [
    # Which account is this entry for?
    "Account",
    # Colored flags
    "Flag",
    # Ignored
    "Check Number",
    # Transaction date
    "Date",
    # Who did you pay / get money from / which account did you transfer money to?
    "Payee",
    # Concatenation of Master and Sub category
    "Category",
    # Category group
    "Master Category",
    "Sub Category",
    # Notes
    "Memo",
    # Amount if positive
    "Outflow",
    # Amount if negative
    "Inflow",
    # Reconciled?
    "Cleared",
    # Ignored
    "Running Balance",
]


@dataclasses.dataclass
class YNABTransaction:
    """
    Source data from YNAB register (refer to YNAB_FIELDS for docs)
    """

    account: str
    flag: str
    date: arrow.Arrow
    payee: str
    category: str
    master_category: str
    sub_category: str
    memo: str
    outflow: Decimal
    inflow: Decimal
    cleared: bool
    running_balance: Decimal


@dataclasses.dataclass
class Config:
    """
    Application configuration
    """

    @dataclasses.dataclass
    class Account:
        class Role(enum.Enum):
            credit_card = "credit_card"
            default = "default"
            savings = "savings"
            cash = "cash"

        # Mapping of Account Name to foreign currency code (ISO 4217)
        # will use default currency if not specified
        currency: str = ""

        # Account roles (credit_card / cash / savings)
        role: Role = Role.default

        # Monthly bill payment date (if not specified will be inferred from transfer transactions)
        monthly_payment_date: str = ""

        # Mark account as inactive after import
        inactive: bool = False

    # All of your YNAB accounts - only need to add here if there's some customization to be done.
    accounts: Dict[str, Account] = dataclasses.field(default_factory=dict)

    # Mapping of currency conversion fallback if foreign amount can't be automatically determined
    currency_conv_fallback: Dict[str, Decimal] = dataclasses.field(default_factory=dict)

    # default currency
    currency: str = "USD"

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
    """
    Processed data to be imported into Firefly
    """

    @dataclasses.dataclass
    class Account:
        class Role(enum.Enum):
            credit_card = "ccAsset"
            default = "defaultAsset"
            savings = "savingAsset"
            cash = "cashWalletAsset"

        name: str
        opening_date: arrow.Arrow
        monthly_payment_date: Optional[arrow.Arrow] = None
        role: Role = Role.default
        opening_balance: Decimal = Decimal(0)
        currency_code: str = "USD"

    @dataclasses.dataclass
    class TransactionGroup:
        @dataclasses.dataclass
        class TransactionMetadata:
            date: arrow.Arrow
            amount: Decimal
            description: str
            notes: str
            tags: List[str]

        @dataclasses.dataclass
        class Withdrawal(TransactionMetadata):
            account: str
            payee: str
            budget: str
            category: str

        @dataclasses.dataclass
        class Deposit(TransactionMetadata):
            account: str
            payee: str
            budget: str
            category: str

        @dataclasses.dataclass
        class Transfer(TransactionMetadata):
            from_account: str
            to_account: str
            # set if exactly one of: from, to is foreign currency account
            foreign_amount: Decimal = None

        title: str = ""
        transactions: List[Union[Withdrawal, Deposit, Transfer]] = dataclasses.field(default_factory=list)

    asset_accounts: List[Account] = dataclasses.field(default_factory=list)
    revenue_accounts: List[str] = dataclasses.field(default_factory=list)
    expense_accounts: List[str] = dataclasses.field(default_factory=list)

    transaction_groups: List[TransactionGroup] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class FireflyData:
    """
    Data in firefly - mostly primary keys (ids) for various objects
    """

    currencies: Dict[str, int] = dataclasses.field(default_factory=dict)
    asset_accounts: Dict[str, int] = dataclasses.field(default_factory=dict)
    revenue_accounts: Dict[str, int] = dataclasses.field(default_factory=dict)
    expense_accounts: Dict[str, int] = dataclasses.field(default_factory=dict)


def create_transaction():
    pass


AMOUNT_RE = re.compile(r"^[^0-9]*([0-9,\\.]+)$")


def _to_amount(s: str) -> Decimal:
    m = AMOUNT_RE.match(s)
    assert m, f"Invalid value with no amount: |{s}|"
    return Decimal(m.group(1).replace(",", ""))


def _ynab_field_name(s: str) -> str:
    return s.lower().replace(" ", "_")


def _is_transfer(tx: YNABTransaction) -> bool:
    return "Transfer : " in tx.payee


def _firefly_compare(obj, firefly_obj) -> bool:
    if isinstance(obj, arrow.Arrow):
        return obj.format("YYYY-MM-DD") == firefly_obj
    elif isinstance(obj, Decimal) and firefly_obj is not None:
        return obj == Decimal(str(firefly_obj))
    elif firefly_obj is None:
        return obj == 0
    return obj == firefly_obj


class Session(requests.Session):
    @staticmethod
    def _json_default(obj):
        if isinstance(obj, Decimal):
            int_val = int(obj)
            if int_val == obj:
                return int_val
            else:
                return float(obj)
        elif isinstance(obj, arrow.Arrow):
            assert not any((obj.hour, obj.minute, obj.second, obj.microsecond))
            return obj.format("YYYY-MM-DD")
        raise TypeError

    def request(self, method, url, **kwargs) -> requests.Response:
        if "json" in kwargs:
            kwargs["data"] = json.dumps(kwargs.pop("json"), default=self._json_default)
        response = super().request(method, url, **kwargs)
        if not response.ok:
            if method == "post" or response.status_code == 500:
                print(response.json())
            response.raise_for_status()
        return response

    def get_all_pages(self, url, params=None, **kwargs) -> dict:
        response = self.get(url, params=params, **kwargs)
        response.raise_for_status()

        all_response = {"data": response.json()["data"]}

        while (
            response.json()["meta"]["pagination"]["current_page"]
            != response.json()["meta"]["pagination"]["total_pages"]
        ):
            if not params:
                params = {}
            params["page"] = response.json()["meta"]["pagination"]["current_page"] + 1

            response = self.get(url, params=params, **kwargs)
            response.raise_for_status()

            all_response["data"].extend(response.json()["data"])

        return all_response


class Importer:
    MEMO_RE = re.compile(r".*([A-Z]{3})\s+([0-9,\\.]+);?.*")

    def __init__(self, config_path, register_path):
        self.firefly_url = os.environ["FIREFLY_III_URL"].rstrip("/")
        self.firefly_token = os.environ["FIREFLY_III_ACCESS_TOKEN"]

        self.config = dacite.from_dict(
            Config, toml.load(config_path), config=dacite.Config(cast=[Decimal, Config.Account.Role], strict=True)
        )
        print(f"Loaded config for import into {self.firefly_url}")

        self.register_path = register_path

        self.data = ImportData()
        self.firefly_data = FireflyData()

        self.all_transactions: List[YNABTransaction] = None

        self._import_tag = f"import-{arrow.now().format('YYYY-MM-DDTHH-MM')}"

        self._session = Session()

    def run(self, dry_run: bool = False):
        if not dry_run:
            self._verify_connection()

        self._read_register()

        self._process_accounts()
        self._process_transactions()

        if not dry_run:
            self._create_currencies()
            self._create_asset_accounts()
            self._create_revenue_accounts()
            self._create_expense_accounts()
            self._create_transactions()

    def _acc_config(self, acc: str) -> Config.Account:
        return self.config.accounts.get(acc, Config.Account())

    def _is_foreign(self, acc: str) -> bool:
        return self._acc_config(acc).currency and self._acc_config(acc).currency != self.config.currency

    def _parse_date(self, dt: str) -> arrow.Arrow:
        return arrow.get(dt, self.config.date_format)

    def _payee(self, tx: YNABTransaction) -> str:
        return self.config.payee_mapping.get(tx.payee, tx.payee)

    def _amount(self, tx: YNABTransaction) -> Decimal:
        # exactly one of these would be non-zero
        amount = tx.outflow - tx.inflow
        if not self._is_foreign(tx.account):
            return amount

        # for foreign accounts, try to use memo to find real value at the time of transaction
        m = self.MEMO_RE.match(tx.memo)
        foreign_currency_code = self._acc_config(tx.account).currency
        if m and m.group(1) == self._acc_config(tx.account).currency:
            # use memo - yaay!
            return Decimal(m.group(2).replace(",", "")) * (-1 if tx.inflow > 0 else 1)

        conv_fallback = self.config.currency_conv_fallback.get(foreign_currency_code)
        if not conv_fallback:
            raise ValueError(
                f"Unable to determine foreign amount for {tx}. Memo must be of form: [CURRENCY CODE] "
                f"[AMOUNT]. Alternatively, set config.currency_conv_fallback."
            )
        return amount * conv_fallback

    def _category(self, tx: YNABTransaction) -> str:
        return getattr(tx, _ynab_field_name(self.config.category_field))

    def _budget(self, tx: YNABTransaction) -> str:
        if tx.master_category == "Hidden Categories":
            return ""
        return getattr(tx, _ynab_field_name(self.config.budget_field))

    def _description(self, tx: YNABTransaction) -> str:
        if not self.config.memo_to_description:
            return self.config.empty_description
        if "(Split" in tx.memo:
            return tx.memo.split(") ")[1].strip() or self.config.empty_description
        return tx.memo.strip() or self.config.empty_description

    def _notes(self, tx: YNABTransaction) -> str:
        if not self.config.memo_to_description:
            return tx.memo.strip()
        return ""

    def _tags(self, tx: YNABTransaction) -> List[str]:
        tags = [self._import_tag]
        if tx.flag:
            tags.append(tx.flag)
        return tags

    def _transfer_account(self, tx: YNABTransaction) -> str:
        if " / Transfer : " in tx.payee:
            return tx.payee.split(" / ")[1].split(" : ")[1]
        return tx.payee.split(" : ")[1]

    def _transfer_foreign_amount(self, tx: YNABTransaction) -> Decimal:
        to_account = self._transfer_account(tx)
        # From foreign account to default currency account --
        if self._is_foreign(tx.account) and not self._is_foreign(to_account):
            # Amount is already computed in `self._amount` above using foreign currency code
            # Outflow / Inflow corresponds to default currency
            return tx.outflow - tx.inflow
        # From default currency account to foreign account
        elif not self._is_foreign(tx.account) and self._is_foreign(to_account):
            # for foreign accounts, try to use memo to find real value at the time of transaction
            m = self.MEMO_RE.match(tx.memo)
            foreign_currency_code = self._acc_config(to_account).currency
            if m and m.group(1) == self._acc_config(to_account).currency:
                # use memo - yaay!
                return Decimal(m.group(2).replace(",", "")) * (-1 if tx.inflow > 0 else 1)

            conv_fallback = self.config.currency_conv_fallback.get(foreign_currency_code)
            if not conv_fallback:
                raise ValueError(
                    f"Unable to determine foreign amount for {tx}. Memo must be of form: [CURRENCY CODE] "
                    f"[AMOUNT]. Alternatively, set config.currency_conv_fallback."
                )
            # Amount is default currency
            amount = tx.outflow - tx.inflow
            return amount * conv_fallback

        elif self._is_foreign(tx.account) and self._is_foreign(to_account):
            assert (
                self._acc_config(tx.account).currency == self._acc_config(to_account).currency
            ), f"Can't handle transaction between two different foreign accounts: {tx}"

    def _read_register(self):
        assert not self.all_transactions, "Already read!"
        with open(self.register_path) as f:
            reader = csv.DictReader(f, fieldnames=YNAB_FIELDS)
            # skip header
            next(reader)
            all_transactions = list(reader)
        print(f"Loaded {len(all_transactions)} transactions")
        self.all_transactions = [
            YNABTransaction(
                account=tx["Account"],
                flag=tx["Flag"],
                date=self._parse_date(tx["Date"]),
                payee=tx["Payee"],
                category=tx["Category"],
                master_category=tx["Master Category"],
                sub_category=tx["Sub Category"],
                memo=tx["Memo"],
                outflow=_to_amount(tx["Outflow"]),
                inflow=_to_amount(tx["Inflow"]),
                cleared=tx["Cleared"] == "R",
                running_balance=_to_amount(tx["Running Balance"]),
            )
            for tx in all_transactions
        ]
        self.all_transactions = sorted(
            # YNAB4 display sorts same date transactions by highest inflow first
            self.all_transactions,
            key=lambda tx: (tx.date, tx.inflow - tx.outflow),
            reverse=True,
        )

    def _process_accounts(self):
        account_names = {tx.account for tx in self.all_transactions}
        for acc in self.config.accounts:
            assert acc in account_names, f"Unknown account with no transactions in config: |{acc}|"

        starting_balances: Dict[str, Tuple[arrow.Arrow, Decimal]] = {
            tx.account: (tx.date, tx.inflow - tx.outflow)
            for tx in self.all_transactions
            if tx.payee == "Starting Balance"
        }

        for acc in account_names:
            start_date, balance = starting_balances[acc]
            account_config = self._acc_config(acc)
            role = ImportData.Account.Role[account_config.role.name]
            monthly_payment_date = None
            if role is ImportData.Account.Role.credit_card:
                if account_config.monthly_payment_date:
                    try:
                        monthly_payment_date = arrow.get(account_config.monthly_payment_date)
                    except arrow.ParserError:
                        try:
                            monthly_payment_date = arrow.get(
                                account_config.monthly_payment_date, self.config.date_format
                            )
                        except arrow.ParserError:
                            raise ValueError(f"Unable to parse date: |{account_config.monthly_payment_date}|")
                else:
                    try:
                        monthly_payment_date = next(
                            tx.date
                            for tx in self.all_transactions
                            if _is_transfer(tx) and self._transfer_account(tx) == acc
                        )
                    except StopIteration:
                        print(f"[WARN] Couldn't figure out monthly payment date for {acc}, defaulting to 01/01")
                        monthly_payment_date = arrow.get("2020-01-01")  # year doesn't matter
            account = ImportData.Account(
                name=acc,
                opening_date=start_date,
                monthly_payment_date=monthly_payment_date,
                role=role,
                opening_balance=balance,
            )
            self.data.asset_accounts.append(account)

        self.data.revenue_accounts = list(
            {self._payee(tx) for tx in self.all_transactions if tx.inflow > 0 and not _is_transfer(tx)}
        )
        self.data.expense_accounts = list(
            {self._payee(tx) for tx in self.all_transactions if tx.outflow > 0 and not _is_transfer(tx)}
        )
        print(
            f"Configured account data for {len(self.data.asset_accounts)} asset accounts, "
            f"{len(self.data.revenue_accounts)} revenue accounts, and {len(self.data.expense_accounts)} "
            f"expense accounts"
        )

    def _process_transactions(self):
        print(f"Transactions will be tagged with {self._import_tag}")

        splits, non_splits = funcy.lsplit(lambda tx: "(Split " in tx.memo, self.all_transactions)
        splits_grouped = defaultdict(list)
        for tx in splits:
            splits_grouped[(tx.account, tx.date, tx.running_balance)].append(tx)
        # process splits first because in case of transfers, we want to split version of Transfer rather than the other
        all_tx_grouped = list(splits_grouped.values())
        all_tx_grouped.extend([[tx] for tx in non_splits])

        # used to de-dup transactions because YNAB will double-log every transfer
        # map key: tuple of accounts (sorted by name), date, abs(outflow - inflow)
        # map value: int - how many times this was seen. Max = 2
        transfers_seen_map: Dict[Tuple[Tuple[str], arrow.Arrow, Decimal]] = {}

        withdrawals_count = deposits_count = transfers_count = 0
        for tx_group in all_tx_grouped:
            transaction_group = ImportData.TransactionGroup()
            if len(tx_group) > 0:
                transaction_group.title = self.config.empty_description

            for tx in tx_group:
                if _is_transfer(tx):
                    transfer_account = self._transfer_account(tx)
                    date = tx.date
                    transfer_seen_map_key = (
                        tuple(sorted([tx.account, transfer_account])),
                        date,
                        abs(tx.outflow - tx.inflow),
                    )
                    if transfers_seen_map.get(transfer_seen_map_key, 0) % 2 == 1:
                        transfers_seen_map[transfer_seen_map_key] += 1
                        continue
                    transfers_seen_map[transfer_seen_map_key] = 1
                    transfer = ImportData.TransactionGroup.Transfer(
                        from_account=tx.account,
                        to_account=transfer_account,
                        date=date,
                        amount=self._amount(tx),
                        description=self._description(tx),
                        foreign_amount=self._transfer_foreign_amount(tx),
                        notes=self._notes(tx),
                        tags=self._tags(tx),
                    )
                    transaction_group.transactions.append(transfer)
                    transfers_count += 1
                elif tx.outflow > 0:
                    withdrawal = ImportData.TransactionGroup.Withdrawal(
                        account=tx.account,
                        date=tx.date,
                        payee=self._payee(tx),
                        amount=self._amount(tx),
                        description=self._description(tx),
                        budget=self._budget(tx),
                        category=self._category(tx),
                        notes=self._notes(tx),
                        tags=self._tags(tx),
                    )
                    transaction_group.transactions.append(withdrawal)
                    withdrawals_count += 1
                elif tx.inflow > 0:
                    deposit = ImportData.TransactionGroup.Deposit(
                        account=tx.account,
                        date=tx.date,
                        payee=self._payee(tx),
                        amount=self._amount(tx),
                        description=self._description(tx),
                        budget=self._budget(tx),
                        category=self._category(tx),
                        notes=self._notes(tx),
                        tags=self._tags(tx),
                    )
                    transaction_group.transactions.append(deposit)
                    deposits_count += 1
            self.data.transaction_groups.append(transaction_group)
        print(
            f"Configured transaction data for {deposits_count} deposits and {withdrawals_count} withdrawals, and "
            f"{transfers_count} transfers in a total of {len(self.data.transaction_groups)} groups."
        )

    def _verify_connection(self) -> None:
        self._session.headers["Accept"] = "application/json"
        self._session.headers["Authorization"] = f"Bearer {self.firefly_token}"
        self._session.headers["Content-Type"] = "application/json"
        response = self._session.get(f"{self.firefly_url}/api/v1/about/user")

        print(f"Authenticated successfully as {response.json()['data']['attributes']['email']}")

    def _create_currencies(self) -> None:
        response = self._session.get_all_pages(f"{self.firefly_url}/api/v1/currencies")
        self.firefly_data.currencies = {d["attributes"]["code"]: int(d["id"]) for d in response["data"]}
        currencies = {d["attributes"]["code"]: d for d in response["data"]}
        currencies_to_keep = [
            "EUR",  # Firefly recommends keeping this always
            self.config.currency,
        ]
        currencies_to_keep.extend([account.currency for account in self.config.accounts.values() if account.currency])
        for code, data in currencies.items():
            if code == self.config.currency:
                if not data["attributes"]["default"]:
                    self._session.post(f"{self.firefly_url}/api/v1/currencies/{code}/default")
            if code in [self.config.currency, "INR", "EUR"]:
                if not data["attributes"]["enabled"]:
                    self._session.post(f"{self.firefly_url}/api/v1/currencies/{code}/enable")
            elif data["attributes"]["enabled"]:
                self._session.post(f"{self.firefly_url}/api/v1/currencies/{code}/disable")

    def _create_asset_accounts(self) -> None:
        response = self._session.get_all_pages(f"{self.firefly_url}/api/v1/accounts")

        existing_account_data = {}
        for data in response["data"]:
            self.firefly_data.asset_accounts[data["attributes"]["name"]] = data["id"]
            existing_account_data[data["attributes"]["name"]] = data

        for account in self.data.asset_accounts:
            data = {
                "name": account.name,
                "active": True,
                "type": "asset",
                "account_role": account.role.value,
                "currency_id": self.firefly_data.currencies[
                    self.config.accounts.get(account.name, Config.Account()).currency or self.config.currency
                ],
                "include_net_worth": True,
            }
            if account.opening_balance:
                # firefly iii will not update date unless balance is non-zero
                data.update({"opening_balance": account.opening_balance, "opening_balance_date": account.opening_date})
            if account.role is ImportData.Account.Role.credit_card:
                data["credit_card_type"] = "monthlyFull"
                data["monthly_payment_date"] = account.monthly_payment_date

            if account.name in self.firefly_data.asset_accounts:
                needs_update = False
                for k, v in data.items():
                    if not _firefly_compare(v, existing_account_data[account.name]["attributes"].get(k)):
                        print(account.name, k, existing_account_data[account.name]["attributes"].get(k), v)
                        needs_update = True
                        break
                if needs_update:
                    self._session.put(
                        f"{self.firefly_url}/api/v1/accounts/{existing_account_data[account.name]['id']}", json=data,
                    )
            else:
                response = self._session.post(f"{self.firefly_url}/api/v1/accounts", json=data)
                self.firefly_data.asset_accounts[account.name] = response.json()["data"]["id"]
        print(f"Created {len(self.firefly_data.asset_accounts)} asset accounts")

    def _create_revenue_accounts(self) -> None:
        pass

    def _create_expense_accounts(self) -> None:
        pass

    def _create_transactions(self) -> None:
        pass


if __name__ == "__main__":
    importer = Importer(sys.argv[1], sys.argv[2])
    importer.run()
