# reguleque-search

## Setup

You should [install Typesense](https://typesense.org/docs/0.20.0/guide/install-typesense.html#%F0%9F%93%A5-download-install) and [Poetry](https://python-poetry.org/docs/#installation), and then to install the requirements use:
```shell
poetry install
```
T
Start a [Typesense](https://typesense.org/) server with:

```shell
typesense-server --data-dir=data/out/ --api-key=<ADMIN-API-KEY>
```

And then load and index the revenue entries on the running instance using the samples provided in `data/in`:

```shell
python3 reguleque_search/main.py
```

The frontend can be found on [agucova/regulf-neo](https://github.com/agucova/regulf-neo).

> **Warning:** The provided API key has full read/write access on collections. A search-only API key must be generated for use with the frontend in production.
