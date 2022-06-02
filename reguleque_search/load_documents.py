import os
from pathlib import Path
from time import sleep
from typing import List
from tqdm import tqdm
from tqdm.utils import CallbackIOWrapper

import requests
import typer
import typesense as ts
from art import tprint
from dask import dataframe as dd
from dask.distributed import Client, progress
from tenacity import retry, wait_fixed, stop_after_attempt, retry_if_result, retry_if_exception_type, RetryError

endpoint = "0.0.0.0"
collection_name = "revenue_entry"
in_path = Path("data/in")
warn_error_tolerance = 0.05

COLUMNS = [
    "id",
    "tipo_contrato",
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

HONORARIOS_COLUMNS = [
    "id",
    "tipo_contrato",
    "nombre_organismo",
    "código_organismo",
    "fecha_publicación",
    "año",
    "mes",
    "nombre",
    "tipo_calificación_profesional",
    "tipo_cargo",
    "región",
    "unidad_monetaria",
    "remuneración_bruta_mensual",
    "remuneración_líquida_mensual",
    "pago_mensual",
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
    "pago_mensual": str
}

REVENUE_SCHEMA = {
    "name": collection_name,
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "nombre", "type": "string"},
        {"name": "tipo_contrato", "type": "string", "facet": True},
        {"name": "nombre_organismo", "type": "string", "facet": True},
        {"name": "código_organismo", "type": "string", "optional": True},
        {"name": "fecha_publicación", "type": "string"},
        {"name": "año", "type": "string", "facet": True},
        {"name": "mes", "type": "string", "facet": True},
        {"name": "tipo_estamento", "type": "string", "facet": True, "optional": True},
        {"name": "tipo_cargo", "type": "string", "optional": True},
        {"name": "tipo_calificación_profesional", "type": "string", "optional": True},
        {"name": "región", "type": "string", "facet": True, "optional": True},
        {"name": "asignaciones", "type": "string", "optional": True},
        {"name": "unidad_monetaria", "type": "string"},
        {"name": "remuneración_bruta_mensual", "type": "float"},
        {"name": "remuneración_líquida_mensual", "type": "float", "optional": True},
        {"name": "horas_diurnas", "type": "string", "optional": True},
        {"name": "horas_nocturnas", "type": "string", "optional": True},
        {"name": "horas_festivas", "type": "string", "optional": True},
        {"name": "pago_mensual", "type": "string", "optional": True},
        {"name": "fecha_ingreso", "type": "string", "optional": True},
        {"name": "fecha_término", "type": "string", "optional": True},
        {"name": "observaciones", "type": "string", "optional": True},
        {"name": "enlace", "type": "string", "optional": True},
        {"name": "viáticos", "type": "string", "optional": True},
    ],
    "default_sorting_field": "remuneración_bruta_mensual",
}

LOG_INFO = typer.style("[INFO]", fg=typer.colors.GREEN, bold=True)
LOG_WARN = typer.style("[WARN]", fg=typer.colors.BRIGHT_YELLOW, bold=True)
LOG_ERR = typer.style("[ERR]", fg=typer.colors.RED, bold=True)
LOG_PROMPT = typer.style("[PROMPT]", fg=typer.colors.WHITE, bold=True)


# @dask.delayed
def load_file(filepath: Path) -> dd.DataFrame:
    columns = HONORARIOS_COLUMNS if "honorarios" in str(filepath) else COLUMNS
    entries = (
        dd.read_csv(
            filepath,
            sep=";",
            names=columns,
            dtype=COLUMN_TYPES,
            header=None,
            skip_blank_lines=True,
            na_filter=True,
            na_values="Indefinido",
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

def non_200(r):
    return r.status_code != 200

@retry(retry=retry_if_result(non_200) | retry_if_exception_type(requests.exceptions.ConnectionError), stop=stop_after_attempt(2), wait=wait_fixed(3), reraise=True)
def import_chunk(chunk, api_key, endpoint, session, port=443, protocol="https"):
    file_size = os.stat(chunk).st_size
    with open(chunk, "rb") as f:
        with tqdm(total=file_size, unit="B", unit_scale=True, unit_divisor=1024) as t:
            wrapped_file = CallbackIOWrapper(t.update, f, "read")
            return session.post(f"{protocol}://{endpoint}:{port}/collections/{collection_name}/documents/import?action=upsert", data=wrapped_file, headers={
                            "X-TYPESENSE-API-KEY": api_key,
                            }, stream=True)
                        

def main(
    endpoint: str = endpoint,
    port: int = 80,
    protocol: str = "http",
    collection_name: str = collection_name,
    in_path: Path = in_path,
    api_key: str = os.getenv("TYPESENSE_API_KEY"),
    drop: bool = False,
    skip_conversion: bool = False
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
    typer.echo(LOG_INFO + f" Connection to Typesense at {protocol}://{endpoint}:{port}")

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
        if not skip_conversion:
            for filepath in filepaths:
                name = Path(filepath).name
                entries: List[dict] = load_file(filepath)
                intermediate_path = Path(f"data/conversion/{name}")
                typer.secho(LOG_INFO + f" Converting {name} to JSONL...")
                if "honorarios" in name:
                    typer.secho(LOG_INFO + f" Honorarios detected, special column schema enabled.")
                dd.DataFrame.to_json(entries, intermediate_path, encoding="utf-8", lines=True)
        
        session = requests.Session()
        for filepath in filepaths:
            name = Path(filepath).name
            typer.secho(LOG_INFO + f" Uploading {name} to typesense instance...")
            name = Path(filepath).name
            chunks = list(Path(f"data/conversion/{name}").glob("*.part"))
            typer.secho(LOG_INFO + f" Using {len(chunks)} chunks...")
            for chunk in chunks:
                try:
                    response = import_chunk(chunk, api_key, endpoint, session, port=port, protocol=protocol)
                except requests.exceptions.ConnectionError as e:
                    typer.echo(LOG_ERR + f" Couldn't establish connection after repeated requests:", err=True)
                    print("   ", e)
                    raise typer.Abort()
                    
                if response.status_code != 200:
                    typer.echo(LOG_ERR + f" Got response code {response.status_code}, with:", err=True)
                    print(response.text)
                    raise typer.Abort()

                entries = response.text.split("\n")
                errors = [line for line in entries if line != "{\"success\":true}"]
                error_ratio = len(errors)/len(entries)
                if error_ratio >= warn_error_tolerance:
                    typer.echo(LOG_WARN + f" {round(error_ratio * 100, 1)}% of entries had errors. Sample:", err=True)
                    print(errors[1])
                
            typer.secho(LOG_INFO + f" All chunks uploaded.")

    except ts.exceptions.RequestUnauthorized:
        typer.echo(LOG_ERR + " Invalid API key or insufficient permissions.", err=True)
        raise typer.Abort()


if __name__ == "__main__":
    typer.run(main)
