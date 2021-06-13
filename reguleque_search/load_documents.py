import json
import fileinput
from itertools import islice
import os
from pathlib import Path
from time import sleep
from typing import List

import requests
import typer
import typesense as ts
from art import tprint
from dask import dataframe as dd
from dask.distributed import Client, progress
from tenacity import retry, wait_fixed, stop_after_attempt, retry_if_exception_type

endpoint = "typesense-lb-5f499a3-1218039079.sa-east-1.elb.amazonaws.com"
collection_name = "revenue_entry"
in_path = Path("data/in")
warn_error_tolerance = 0.05

COLUMNS = [
    "tipo_contrato",
    "path",
    "nombre_organismo",
    "código_organismo",
    "fecha_publicación",
    "año",
    "mes",
    "tipo_estamento",
    "nombre",
    "tipo_calificación_profesional",
    "tipo_cargo",
    "región",
    "asignaciones",
    "unidad_monetaria",
    "remuneración_bruta_mensual",
    "remuneración_líquida_mensual",
    "horas_diurnas",
    "horas_nocturnas",
    "horas_festivas",
    "fecha_ingreso",
    "fecha_término",
    "observaciones",
    "enlace",
    "viáticos",
]

COLUMN_TYPES = {
    "id": str,
    "tipo_contrato": str,
    "nombre_organismo": str,
    "código_organismo": str,
    "fecha_publicación": str,
    "año": str,
    "mes": str,
    "tipo_estamento": str,
    "nombre": str,
    "tipo_calificación_profesional": str,
    "tipo_cargo": str,
    "región": str,
    "asignaciones": str,
    "unidad_monetaria": str,
    "remuneración_bruta_mensual": float,
    "remuneración_líquida_mensual": float,
    "horas_diurnas": str,
    "horas_nocturnas": str,
    "horas_festivas": str,
    "fecha_ingreso": str,
    "fecha_término": str,
    "observaciones": str,
    "enlace": str,
    "viáticos": str,
}

REVENUE_SCHEMA = {
    "name": collection_name,
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "nombre", "type": "string"},
        {"name": "tipo_contrato", "type": "string", "facet": True},
        {"name": "nombre_organismo", "type": "string", "facet": True},
        {"name": "código_organismo", "type": "string"},
        {"name": "fecha_publicación", "type": "string"},
        {"name": "año", "type": "string", "facet": True},
        {"name": "mes", "type": "string", "facet": True},
        {"name": "tipo_estamento", "type": "string", "facet": True},
        {"name": "tipo_cargo", "type": "string"},
        {"name": "tipo_calificación_profesional", "type": "string"},
        {"name": "región", "type": "string", "facet": True},
        {"name": "asignaciones", "type": "string"},
        {"name": "unidad_monetaria", "type": "string"},
        {"name": "remuneración_bruta_mensual", "type": "float"},
        {"name": "remuneración_líquida_mensual", "type": "float"},
        {"name": "horas_diurnas", "type": "string"},
        {"name": "horas_nocturnas", "type": "string"},
        {"name": "horas_festivas", "type": "string"},
        {"name": "fecha_ingreso", "type": "string"},
        {"name": "fecha_término", "type": "string"},
        {"name": "observaciones", "type": "string"},
        {"name": "enlace", "type": "string"},
        {"name": "viáticos", "type": "string"},
    ],
    "default_sorting_field": "remuneración_líquida_mensual",
}

LOG_INFO = typer.style("[INFO]", fg=typer.colors.GREEN, bold=True)
LOG_WARN = typer.style("[WARN]", fg=typer.colors.BRIGHT_YELLOW, bold=True)
LOG_ERR = typer.style("[ERR]", fg=typer.colors.RED, bold=True)
LOG_PROMPT = typer.style("[PROMPT]", fg=typer.colors.WHITE, bold=True)


# @dask.delayed
def load_file(filepath: Path) -> dd.DataFrame:
    entries = (
        dd.read_csv(
            filepath,
            sep=";",
            names=COLUMNS,
            dtype=COLUMN_TYPES,
            header=None,
            skip_blank_lines=True,
            na_filter=True,
            na_values="Indefinido",
            # dayfirst=True,
            encoding="ISO-8859-1",
            # index_col="id",
        )
        .dropna(how="all")
    )
    for col in entries:
        #get dtype for column
        dt = entries[col].dtype 
        #check if it is a number
        if dt == int or dt == float:
            entries[col] = entries[col].fillna(0)
        else:
            entries[col] = entries[col].fillna("")

    return entries

def import_chunk(f, api_key, endpoint, port=443, protocol="https"):
    return requests.post(f"{protocol}://{endpoint}:{port}/collections/{collection_name}/documents/import?action=create", data=f, headers={
                        "X-TYPESENSE-API-KEY": api_key,
                        }, stream=True)
@retry(stop=stop_after_attempt(4), wait=wait_fixed(2))
def import_lines(lines, api_key, endpoint, port=443, protocol="https"):
    return requests.post(f"{protocol}://{endpoint}:{port}/collections/{collection_name}/documents/import?action=create", data="".join(lines), headers={
                            "X-TYPESENSE-API-KEY": api_key,
                        })
def main(
    endpoint: str = endpoint,
    port: int = 80,
    protocol: str = "http",
    collection_name: str = collection_name,
    in_path: Path = in_path,
    api_key: str = os.getenv("TYPESENSE_API_KEY"),
    drop: bool = False,
):
    tprint("Reguleque")
    tsClient = ts.Client(
        {
            "api_key": api_key
            or typer.prompt(LOG_PROMPT + " Typesense Admin API Key", type=str),
            "nodes": [
                {
                    "host": os.getenv("TYPESENSE_HOST") or endpoint,
                    "port": port,
                    "protocol": protocol,
                }
            ],
            "retry_interval_seconds": 15,
            "connection_timeout_seconds": 90,
            "num_retries": 10
        }
    )
    typer.echo(LOG_INFO + f" Connected to Typesense at {protocol}://{endpoint}:{port}")

    daskClient = Client()
    typer.echo(
        LOG_INFO + f" Started cluster, you can monitor at {daskClient.dashboard_link}"
    )

    # List all the files that need loading
    filepaths = list(in_path.rglob("**/*.csv"))
    typer.secho(LOG_INFO + f" Found {len(filepaths)} files to load.")

    try:
        # Drop pre-existing collection if any
        if drop:
            confirm_drop = typer.confirm(
                LOG_WARN
                + " Are you sure you want to delete all documents in the cluster and recreate the schema?"
            )
            if not confirm_drop:
                typer.echo(LOG_ERR + " Canceling execution.", err=True)
                raise typer.Abort()
            typer.echo(
                LOG_WARN
                + " Drop mode has been enabled, dropping all documents and recreating schema...",
                err=True,
            )
            try:
                tsClient.collections[collection_name].delete()
            except Exception:
                pass 
            # Create collection with the manual schema
            tsClient.collections.create(REVENUE_SCHEMA)
            typer.secho(LOG_INFO + " Created new schema.")

        # Load all files
        for filepath in filepaths:
            name = Path(filepath).name
            entries: List[dict] = load_file(filepath)
            intermediate_path = Path(f"data/conversion/{name}")
            typer.secho(LOG_INFO + f" Converting {name} to JSONL...")
            dd.DataFrame.to_json(entries, intermediate_path, encoding="utf-8", lines=True)
        
        # responses: List[List[str]] = []
        # typer.secho(LOG_INFO + f" Uploading {name} to typesense instance...")
        # for filepath in filepaths:
        #     name = Path(filepath).name
        #     chunks = Path(f"data/conversion/{name}").glob("*.part")
        #     for chunk in chunks:
        #         ## STREAMING
        #         responses = []
        #         # with open(chunk, "rb") as f:
        #         #     response = import_chunk(f, api_key, endpoint, port=port, protocol=protocol)
        #         #     responses.append(response)
        #         # typer.secho(LOG_INFO + f" Chunks uploaded, results: {responses}")

        #         ## CHUNKED
        #         with open(chunk) as jsonl_file:
        #             i = 0
        #             while True:
        #                 print(f"chunk {i}")
        #                 next_n_lines = list(islice(jsonl_file, 2000))
        #                 if not next_n_lines:
        #                     break
        #                 i += 1
        #                 print("pre")
        #                 # response: List[str] = tsClient.collections[collection_name].documents.import_(next_n_lines)
        #                 response = import_lines(next_n_lines, api_key, endpoint, port=port, protocol=protocol)
        #             # sleep(10)
        #             responses.append(response)

        # typer.secho(
        #     "\n"
        #     + LOG_INFO
        #     + f" Finished processing and uploading {len(filepaths)} documents."
        # )
    except ts.exceptions.RequestUnauthorized:
        typer.echo(LOG_ERR + " Invalid API key or insufficient permissions.", err=True)
        raise typer.Abort()


if __name__ == "__main__":
    typer.run(main)
