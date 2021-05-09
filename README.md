# reguleque-search

## Setup


Start the TypeSense server with:

```shell
typesense-server --data-dir=data/out/ --api-key=<ADMIN-API-KEY>
```

And then load and index the revenue entries on the running instance (samples provided in `data/in`):

```shell
python3 reguleque_search/main.py
```

The frontend can be found on [agucova/regulf-neo](https://github.com/agucova/regulf-neo).

> **Warning:** The provided API key has full read/write access on collections. A search-only API key must be generated for use with the frontend in production.
