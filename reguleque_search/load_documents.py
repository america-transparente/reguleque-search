import json
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
    "id",
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
    return entries

# @dask.delayed
def import_entries(
    entries: dd.DataFrame, filepath: Path, tsClient, action: str = "create"
) -> List[str]:
    # Needed to transform from Int64 (pandas) to native int for     JSON serialization. Int64 was needed to allow NAs
    # entries["grado_eus"] = entries["grado_eus"].astype(int)
    entries = entries.to_dict(orient="records")
    print("triggered import_entries with ", filepath)
    generic_success = '"{"success":true}"'
    try:
        api_responses = [
            response.replace("\\", "")
            for response in tsClient.collections[collection_name].documents.import_(
                entries, {"action": action}
            )
        ]
    except ts.exceptions.RequestUnauthorized:
        typer.echo(
            "\n" + LOG_ERR + " Invalid API key or insufficient permissions.", err=True
        )
        raise typer.Abort()
    # except requests.exceptions.ConnectionError:
    #     typer.echo("\n" + LOG_ERR + " Error in connection.", err=True)
    #     raise typer.Abort()
    errors = [response for response in api_responses if response != generic_success]
    if len(errors) / len(api_responses) > warn_error_tolerance:
        typer.echo(
            "\n"
            + LOG_WARN
            + f" Found problem with {len(errors)}/{len(api_responses)} files while uploading {filepath.as_posix()}",
            err=True,
        )
        typer.echo("\n" + LOG_WARN + f" Sample output: {errors[-1]}", err=True)
    return dd.Series(api_responses)

def remove_empty_elements(d):
    """recursively remove empty lists, empty dicts, or None elements from a dictionary"""

    def empty(x):
        return x is None or x == {} or x == []

    if not isinstance(d, (dict, list)):
        return d
    elif isinstance(d, list):
        return [v for v in (remove_empty_elements(v) for v in d) if not empty(v)]
    else:
        return {k: v for k, v in ((k, remove_empty_elements(v)) for k, v in d.items()) if not empty(v)}


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
        
        responses: List[List[str]] = []
        for filepath in filepaths:
            name = Path(filepath).name
            chunks = Path(f"data/conversion/{name}").glob("*.part")
            typer.secho(LOG_INFO + f" Uploading {name} to typesense instance...")
            for chunk in chunks:
                typer.secho(LOG_INFO + f" Loading {str(chunk)}")
                # ## Remove nulls
                # with open(chunk, "r") as f:
                #     line = f.readline()
                    
                #     while line:
                #         line = remove_empty_elements(json.loads(line))
                #         line = f.readline()


                ## STREAMING
                with open(chunk, "rb") as f:
                    requests.post(f"{protocol}://{endpoint}:{port}/collections/{collection_name}/documents/import?action=create", data=f, headers={
                        "X-TYPESENSE-API_KEY": api_key,
                        }, timeout=20)

                # ## CHUNKED
                # with open(chunk) as jsonl_file:
                #     while True:
                #         next_n_lines = list(islice(jsonl_file, 2000))
                #         if not next_n_lines:
                #             break
                #         # response: List[str] = tsClient.collections[collection_name].documents.import_(next_n_lines)
                #         requests.post(f"{protocol}://{endpoint}:{port}/collections/{collection_name}/documents/import?action=create", data="".join(next_n_lines), headers={
                #             "X-TYPESENSE-API_KEY": api_key,
                #         }, timeout=10)
                #     sleep(10)
                    # responses.append(response)

        typer.secho(
            "\n"
            + LOG_INFO
            + f" Finished processing and uploading {len(filepaths)} documents."
        )
    except ts.exceptions.RequestUnauthorized:
        typer.echo(LOG_ERR + " Invalid API key or insufficient permissions.", err=True)
        raise typer.Abort()


if __name__ == "__main__":
    typer.run(main)
