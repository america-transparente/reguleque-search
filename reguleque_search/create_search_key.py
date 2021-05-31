import os
import typesense as ts

client = ts.Client(
    {
        "api_key": os.getenv("TYPESENSE_API_KEY") or input("TypeSense Admin API Key: "),
        "nodes": [{"host": "api.reguleque.cl", "port": "443", "protocol": "https"}],
    }
)

print(
    "=>  Generated Search-only API Key:",
    client.keys.create(
        {
            "description": "Search-only reguleque key.",
            "actions": ["documents:search"],
            "collections": ["revenue_entry"],
        }
    )["value"],
)
