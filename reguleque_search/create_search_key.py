import os
import typesense as ts

client = ts.Client(
    {
        "api_key": os.getenv("TYPESENSE_API_KEY") or input("TypeSense Admin API Key: "),
        "nodes": [{"host": "0.0.0.0", "port": "80", "protocol": "http"}],
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
