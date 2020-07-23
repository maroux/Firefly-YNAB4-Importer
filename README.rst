Firefly Importer Importer
=========================

.. image:: https://img.shields.io/pypi/v/Firefly-YNAB4-Importer.svg?style=flat-square
    :target: https://pypi.python.org/pypi/Firefly-YNAB4-Importer

.. image:: https://img.shields.io/pypi/pyversions/Firefly-YNAB4-Importer.svg?style=flat-square
    :target: https://pypi.python.org/pypi/Firefly-YNAB4-Importer

.. image:: https://img.shields.io/pypi/implementation/Firefly-YNAB4-Importer.svg?style=flat-square
    :target: https://pypi.python.org/pypi/Firefly-YNAB4-Importer

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
    :target: https://github.com/ambv/black

Simple importer for moving from YNAB4_ (You Need A Budget) to `Firefly-iii`_.

What?
-----

This tool lets you migrate your financial history from YNAB 4 (not nYNAB) to Firefly-iii with minimal manual actions.
It's written in Python and has the following features:

- Create asset accounts, budgets, budget history, categories, revenue accounts, expense accounts.
- Import splits accurately!
- Automatically verifies the integrity of import by comparing Running Balance in YNAB 4 to Firefly iii
- Foreign currency support - convert to foreign amounts in Firefly-iii seamlessly. Also gets real amounts from memo if
  that's part of your YNAB workflow.
- Idempotent imports! Ran into an error mid-import? Just run it again after correcting the problem.
- Handle inactive budgets
- Caches firefly data so re-runs are fast

These cases are unsupported / not on the roadmap:

- Importing budget limits - YNAB 4's secret sauce is the rules_.
  One of those rules - live on last month's income doesn't match how Firefly budgets things. So this will require some
  more thought / investigation on how to set up sanely.
- Multiple foreign currencies (rare, but possible)

Why?
----

YNAB 4 is (was?) a desktop software with support for sharing using external mechanisms like Dropbox or just local file
sharing like Airdrop. After version 4, YNAB went online with monthly subscription, all data on the cloud and
`stopped supporting`_ YNAB 4. If you don't like your financial data on the cloud, very few alternatives exist, and
Firefly is one of the solid ones.

Firefly iii gives you control over where you want to host the application - local / in your own cloud etc, provides
most of the features that YNAB 4 did and then some. Reports in particular are :100:.

However, if you have historical data in YNAB 4 like I do, starting fresh means throwing away all that history. Firefly
iii does support migration from nYNAB (the cloud version) natively, but not from YNAB 4. There's also a
`csv importer`_ but it misses a lot of things that this tool fixes, such as:

1. Doesn't import budget history.
#. Need manual management of accounts (otherwise it gets confused about "Citi" the asset account with "Citi" the expense
   account).
#. Transfers get duplicated because YNAB stores them as two transactions.
#. Splits are a whole can of worms.
#. No support for foreign transactions
#. Flaky import - doesn't really work for imports of more than 500 transactions at a time and fails intermittently.

How?
----

Pre-requisites:

1. Python 3.8+

Setup:

1. Install: ``pip install Firefly-YNAB4-Importer``
#. Export YNAB 4 data to local disk
#. Setup config (see ``config.example.toml`` for documentation)
#. Backup Firefly iii database! (IMPORTANT!)

   - Either using `Firefly iii export`_, or just backing up your SQL database.
#. Run import:

   .. code-block:: sh

     export FIREFLY_III_URL=<firefly url>
     export FIREFLY_III_ACCESS_TOKEN=<firefly access token>
     firefly-ynab4-importer import <config file> "<register path>" "<budget path>"

   where:

   - ``<firefly url>`` is the url for your firefly installation
   - ``<firefly access token>`` is the `personal access token`_ for your user
   - ``<config file>`` is the path to config file created earlier
   - ``<register path>`` is the path to the YNAB export register file (the one named
      ``<budge name> as of <timestamp>-Register.csv``)
   - ``<budget path>`` is the path to the YNAB export budget file (the one
      named ``<budge name> as of <timestamp>-Budget.csv``).
   - Remember to double quote since that path contains spaces.

#. Additional options:

   - Limit imports to certain dates (this is useful in verifying that import works fine for your use case)

     .. code-block:: sh

       firefly-ynab4-importer import <config file> "<register path>" "<budget path>" "<start month>" "<end month>"

Development
-----------

Setup:

1. Install python 3.8+ using favorite tool e.g. Pyenv_.
#. Optionally create virtualenv using your favorite method e.g. `Pyenv virtualenv`_.
#. Install requirements: ``pip install -e .[dev]``
#. Verify `Firefly-YNAB4-Importer` runs cleanly.
#. Publish new version:

   .. code-block:: sh

     python setup.py sdist bdist_wheel

     twine upload dist/*

Bugs
----

We use GitHub issues for tracking bugs and feature requests. YNAB 4 and Firefly iii are both fairly complicated
software. It's not only possible, but likely that you'll run into issues if your setup is moderate to high level of
customization.

If you find a bug, please `open an issue`_.

Contributing
------------

If you find this useful and want to contribute, here's a list of feature I'd like to add -

- Import Reconciliation transactions correctly
- Investigate budget history support
- Multiple foreign currencies (rare, but possible)
- Command to clear cache, and move cache to appropriate directory depending on platform (e.g. ``~/.cache`` on Unix)
- Better error handling - guide user on how to correct problems
- Testing 

  - Set up test fixtures for inputs and expected outputs
  - Verify all the options in Config work correctly
- Type checking - mypy checks.


.. _YNAB4: https://www.youneedabudget.com/
.. _Firefly-iii: https://firefly-iii.org/
.. _rules: https://www.youneedabudget.com/the-four-rules/
.. _stopped supporting: https://www.youneedabudget.com/ynab-4-support-will-end-october-2019/
.. _csv importer: https://firefly-iii.gitbook.io/firefly-iii-csv-importer/
.. _Firefly iii export: https://docs.firefly-iii.org/exporting-data/export
.. _personal access token: https://docs.firefly-iii.org/api/api#personal-access-token
.. _Pyenv: https://github.com/pyenv/pyenv-installer
.. _Pyenv virtualenv: https://github.com/pyenv/pyenv-virtualenv
.. _open an issue: https://github.com/maroux/YNAB4-Firefly-iii-Exporter/issues/new
.. _click: https://click.palletsprojects.com/en/7.x/
