# reguleque-search

## Setup

You should [install Typesense](https://typesense.org/docs/0.20.0/guide/install-typesense.html#%F0%9F%93%A5-download-install) and [Poetry](https://python-poetry.org/docs/#installation). To install the requirements:
```shell
poetry install
```

Then start a Typesense instance with:


```shell
typesense-server --data-dir=data/out/ --api-key=<ADMIN-API-KEY>
```

> **Warning:** The provided API key has full read/write access on collections. A search-only API key must be generated for use with the frontend in production.

And then load and index the revenue entries on the running instance using the samples provided in `data/in`:

```shell
python reguleque_search/main.py
```

The accompanying frontend can be found on [agucova/regulf-neo](https://github.com/agucova/regulf-neo).
