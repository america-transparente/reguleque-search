import os
from typing import List
from art import tprint
import typesense as ts
import pandas as pd
import dask
from dask.distributed import Client, progress
from pathlib import Path
import typer
from time import sleep

endpoint = "api.reguleque.cl"
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
    "grado_eus",
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
    "path": str,
    "nombre_organismo": str,
    "código_organismo": str,
    "fecha_publicación": str,
    "año": str,
    "mes": str,
    "tipo_estamento": str,
    "nombre": str,
    "grado_eus": "Int64",
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
        {"name": "path", "type": "string"},
        {"name": "nombre_organismo", "type": "string", "facet": True},
        {"name": "código_organismo", "type": "string"},
        {"name": "fecha_publicación", "type": "string"},
        {"name": "año", "type": "string", "facet": True},
        {"name": "mes", "type": "string", "facet": True},
        {"name": "tipo_estamento", "type": "string", "facet": True},
        {"name": "grado_eus", "type": "int32", "facet": True},
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


@dask.delayed
def process_file(filepath: Path) -> List[dict]:
    with open(filepath, "r", encoding="ISO-8859-1") as f:
        entries = (
            pd.read_csv(
                f,
                sep=";",
                names=COLUMNS,
                dtype=COLUMN_TYPES,
                header=None,
                skip_blank_lines=True,
                na_filter=True,
                na_values="Indefinido",
                dayfirst=True,
                encoding="ISO-8859-1",
                index_col="id",
            )
            .dropna(axis=0, how="all")
            .fillna("")
        )
    # Needed to transform from Int64 (pandas) to native int for     JSON serialization. Int64 was needed to allow NAs
    entries["grado_eus"] = entries["grado_eus"].astype(int)
    entries = entries.to_dict(orient="records")
    return entries


@dask.delayed
def import_entries(
    entries: List[dict], filepath: Path, tsClient, action: str = "upsert"
) -> List[str]:
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
    errors = [response for response in api_responses if response != generic_success]
    if len(errors) / len(api_responses) > warn_error_tolerance:
        typer.echo(
            "\n"
            + LOG_WARN
            + f" Found problem with {len(errors)}/{len(api_responses)} files while uploading {filepath.as_posix()}",
            err=True,
        )
        typer.echo("\n" + LOG_WARN + f" Sample output: {errors[-1]}", err=True)
    return api_responses


def main(
    endpoint: str = endpoint,
    port: int = 443,
    protocol: str = "https",
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
        typer.secho(LOG_INFO + " Processing and uploading documents...")
        responses: List[List[str]] = []
        for filepath in filepaths:
            entries: List[dict] = process_file(filepath)
            response: List[str] = import_entries(entries, filepath, tsClient)
            responses.append(response)

        responses = daskClient.persist(responses)
        progress(responses)
        responses = daskClient.gather(responses)
        sleep(2)
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
