# YNAB4-Firefly-iii-Export

Simple importer for moving from [YNAB4](https://www.youneedabudget.com/) to [Firefly-iii](https://firefly-iii.org/).

## Why?

YNAB 4 is (was?) a desktop software with support for sharing using external mechanisms like Dropbox. After v4, YNAB went
online with monthly subscription and all data on the cloud and
[stopped supporting](https://www.youneedabudget.com/ynab-4-support-will-end-october-2019/) YNAB 4. If you don't like
your financial data on the cloud, very few alternatives exist, and Firefly is one of the solid ones.

Firefly iii gives you control over where you want to host the application - local / in your own cloud etc. However, if
you have historical data in YNAB 4 like I do, starting fresh means throwing away all that history. Firefly iii does
support migration from nYNAB (the cloud version) natively, but not from YNAB 4. There's also a
[csv importer](https://firefly-iii.gitbook.io/firefly-iii-csv-importer/) but it misses a lot of things:

1. Doesn't import budget history
2. Need manual management of accounts (otherwise it gets confused about Citi the asset account with Citi the expense
   account).
3. Transfers get duplicated because YNAB stores them as two transactions.
4. Splits are a whole can of worms.
5. No support for foreign transactions if you used memo to store foreign amounts.

## What?

This simple tool written in Python, has the following features:

1. Create accounts, budgets, budget history, categories, revenue accounts, expense accounts. 
1. Foreign currencies are supported and converts to foreign amounts in Firefly-iii seamlessly.
1. Import splits accurately!
1. Idempotent imports! Ran into an error mid-import? Just run it again after correcting the problem.
1. Handle inactive budgets
1. Automatically verifies the integrity of import by comparing Running Balance in YNAB 4 to Firefly iii
1. Caches firefly data so re-runs are fast

These cases are unsupported / not on the roadmap:

1. Importing budget limits - YNAB 4's secret sauce if the [rules](https://www.youneedabudget.com/the-four-rules/).
   One of those rules - live on last month's income doesn't match how Firefly budgets things.

## How?

Pre-requisites:
1. Python 3.8+

Setup:

1. Install:
   ```shell script
   pip install ynab4-firefly-exporter
   ```
1. Export YNAB 4 data to local disk
1. Setup config (see [`config.example.toml`](config.example.toml) for documentation)
1. Backup Firefly iii database! (IMPORTANT!)
   - Either using [Firefly iii export](https://docs.firefly-iii.org/exporting-data/export), or just backing up your SQL
     database. 
1. Run import:
    ```shell script
    export FIREFLY_III_URL=<firefly url>
    export FIREFLY_III_ACCESS_TOKEN=<firefly access token>
    ynab4-firefly-exporter <config file> "<register path>" "<budget path>"
    ```
    where:
    - `<firefly url>` is the url for your firefly installation
    - `<firefly access token>` is the 
       [personal access token](https://docs.firefly-iii.org/api/api#personal-access-token) for your user
    - `<config file>` is the path to config file created earlier 
    - `<register path>` is the path to the YNAB export register file (the one named
    `<budge name> as of <timestamp>-Register.csv`)
    - `<budget path>` is the path to the YNAB export budget file (the one 
    named `<budge name> as of <timestamp>-Budget.csv`).
    - Remember to double quote since that path contains spaces.
    
    Additional options:
    - Limit imports to certain dates (this is useful in verifying that import works fine for your use case)
      ```shell script
      ynab4-firefly-exporter <config file> "<register path>" "<budget path>" "<start month>" "<end month>"
      ```

## Development

Setup:

1. Install python 3.8+ using favorite tool e.g. [Pyenv](https://github.com/pyenv/pyenv-installer).
1. Optionally create virtualenv using your favorite method e.g. [Pyenv virtualenv](https://github.com/pyenv/pyenv-virtualenv).
1. Install python [requirements](requirements.txt).
1. Verify `python -m main` runs cleanly.

## Bugs

We use GitHub issues for tracking bugs and feature requests. YNAB 4 and Firefly iii are both fairly complicated
software. It's not only possible, but likely that you'll run into issues if your setup is moderate to high level of
customization.

If you find a bug, please [open an issue](https://github.com/maroux/YNAB4-Firefly-iii-Export/issues/new).

## Contributing

If you find this useful and want to contribute, here's a list of feature I'd like to add -

- Move to [click](https://click.palletsprojects.com/en/7.x/) + better CLI documentation
- Investigate budget history support
- Mark accounts as inactive automatically
- Multiple foreign currencies (rare, but possible)
- Command to clear cache, and move cache to appropriate directory depending on platform (e.g. `~/.cache` on Unix)
- Better error handling - guide user on how to correct problems
