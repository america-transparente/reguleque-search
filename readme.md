# reguleque-search

Scripts for tidying and indexing documents for [Reguleque](https://reguleque.cl), a search engine for public worker transparency records from the Chilean government.

## Requirements
To install the requirements with [Poetry](https://python-poetry.org/docs/#installation):

```shell
poetry install
```

Alternatively, install them with pip (may be outdated):

```shell
pip3 install -r requirements.txt
```

## Uploading to production

To upload to production, remove the example files from `data/in` and add your own CSV files. (They can be in subfolders, as long as the extension is `.csv`).

Then, run the script as follows:

```shell
python3 reguleque_search/load_documents.py
```

You'll be asked to enter the admin API key for the public endpoint (you can also use the environment variable `TYPESENSE_API_KEY` for this).

By default, the script will not modify existing documents or the collection schema. To overwrite the default schema and drop all documents:

> **Warning:** This will delete all the documents in the Typesense collection.

```shell
python3 reguleque_search/load_documents.py --drop
```

## Local testing

You should [install Typesense](https://typesense.org/docs/0.20.0/guide/install-typesense.html#%F0%9F%93%A5-download-install).

Then start a Typesense instance with:

```shell
typesense-server --data-dir=data/out/ --api-key=<ADMIN-API-KEY>
```

> **Warning:** The provided API key has full read/write access on collections. A search-only API key must be generated for use with the frontend in production.

And then load and index the revenue entries on the running instance using the samples provided in `data/in`:

```shell
python3 reguleque_search/load_documents.py --protocol http --endpoint 127.0.0.1 --port 8108
```

The accompanying frontend can be found on [agucova/regulf-neo](https://github.com/agucova/regulf-neo).
