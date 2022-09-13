import os
import time
from pathlib import Path
from typing import Union

import meilisearch as ms
import requests
import typer
from art import tprint
from dask.dataframe.core import DataFrame
from distributed.client import Client
from tenacity import retry
from tenacity.retry import retry_if_exception_type, retry_if_result
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_fixed
from tqdm import tqdm
from tqdm.utils import CallbackIOWrapper

from load import load_file

endpoint = "0.0.0.0"
collection_name = "reguleque"
warn_error_tolerance = 0.05

LOG_INFO = typer.style("[INFO]", fg=typer.colors.GREEN, bold=True)
LOG_WARN = typer.style("[WARN]", fg=typer.colors.BRIGHT_YELLOW, bold=True)
LOG_ERR = typer.style("[ERR]", fg=typer.colors.RED, bold=True)
LOG_PROMPT = typer.style("[PROMPT]", fg=typer.colors.WHITE, bold=True)


def non_ok(r):
    return r.status_code not in (200, 202)


@retry(
    retry=retry_if_result(non_ok)
    | retry_if_exception_type(requests.exceptions.ConnectionError),
    stop=stop_after_attempt(5),
    wait=wait_fixed(3),
    reraise=True,
)
def import_chunk(
    chunk, api_key, endpoint, session, port=443, protocol="https"
) -> requests.Response:
    file_size = os.stat(chunk).st_size
    with open(chunk, "rb") as f:
        with tqdm(total=file_size, unit="B", unit_scale=True, unit_divisor=1024) as t:
            wrapped_file = CallbackIOWrapper(t.update, f, "read")
            return session.post(
                f"{protocol}://{endpoint}:{port}/indexes/{collection_name}/documents",
                data=wrapped_file,
                headers={
                    "Content-Type": "application/x-ndjson",
                    "Authorization": f"Bearer {api_key}" if api_key else None,
                },
                stream=True,
            )


def main(
    in_path: Path,
    intermediate_path: Path = Path("data/conversion/"),
    endpoint: str = endpoint,
    port: int = 7700,
    protocol: str = "http",
    collection_name: str = collection_name,
    api_key: Union[str, None] = os.getenv("MEILISEARCH_API_KEY") or None,
    drop: bool = False,
    skip_conversion: bool = False,
):
    tprint("Reguleque")
    if api_key:
        msClient = ms.Client(f"{protocol}://{endpoint}:{port}", api_key)
    else:
        msClient = ms.Client(f"{protocol}://{endpoint}:{port}")
    try:
        health = msClient.health()["status"]
        if not health == "available":
            typer.echo(
                LOG_ERR
                + f" The Meilisearch endpoint doesn't seem available. Status: {health}"
            )

    except ms.errors.MeiliSearchCommunicationError:
        typer.echo(LOG_ERR + " Could not connect to MeiliSearch endpoint.")
        return

    typer.echo(
        LOG_INFO + f" Connection to Meilisearch at {protocol}://{endpoint}:{port}"
    )

    daskClient = Client()
    typer.echo(
        LOG_INFO + f" Started cluster, you can monitor at {daskClient.dashboard_link}"
    )

    # List all the files that need loading
    filepaths = list(in_path.rglob("**/*.csv"))
    typer.secho(LOG_INFO + f" Found {len(filepaths)} files to load.")

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
            msClient.index(collection_name).delete()
        except Exception:
            pass
        # Create collection
        msClient.index(collection_name).create(
            msClient.config, collection_name, {"primaryKey": "id"}
        )
        msClient.index(collection_name).update_sortable_attributes(
            ["remuneración_líquida_mensual", "remuneración_bruta_mensual", "grado_eus"]
        )
        msClient.index(collection_name).update_filterable_attributes(
            ["año", "mes", "nombre_organismo", "tipo_contrato"]
        )
        typer.secho(LOG_INFO + " Created new schema.")

    # Load all files
    if not skip_conversion:
        for filepath in filepaths:
            name = Path(filepath).name
            entries: DataFrame = load_file(filepath)
            output_path = intermediate_path / name
            typer.secho(LOG_INFO + f" Converting {name} to JSONL...")
            DataFrame.to_json(entries, output_path, encoding="utf-8", lines=True)

    session = requests.Session()
    for filepath in filepaths:
        name = Path(filepath).name
        typer.secho(LOG_INFO + f" Uploading {name} to MeiliSearch instance...")
        name = Path(filepath).name
        chunks = list(Path(f"data/conversion/{name}").glob("*.part"))
        typer.secho(LOG_INFO + f" Using {len(chunks)} chunks...")

        tasks = []

        for chunk in chunks:
            try:
                response = import_chunk(
                    chunk, api_key, endpoint, session, port=port, protocol=protocol
                )
            except requests.exceptions.ConnectionError as e:
                typer.echo(
                    LOG_ERR
                    + f" Couldn't establish connection after repeated requests:",
                    err=True,
                )
                print("   ", e)
                raise typer.Abort()

            if response.status_code not in (200, 202):
                typer.echo(
                    LOG_ERR + f" Got response code {response.status_code}, with:",
                    err=True,
                )
                print(response.text)

            response = response.json()
            tasks.append(response["taskUid"])
            typer.echo(LOG_INFO + f" Uploaded chunk {chunk.name}.")

        typer.secho(LOG_INFO + f" All chunks uploaded.")

        typer.echo(LOG_INFO + " Waiting for all chunks to be processed...")
        with tqdm(total=len(tasks)) as t:
            while tasks:
                for task in tasks:
                    taskResponse = msClient.get_task(task)
                    if taskResponse["status"] == "succeeded":
                        t.update(1)
                        tasks.remove(task)
                    elif taskResponse["status"] == "failed":
                        typer.echo(
                            LOG_ERR + f" Task {task} failed with:", err=True
                        )
                        print(f"   {taskResponse['error']['message']}")
                        tasks.remove(task)

                time.sleep(2)



if __name__ == "__main__":
    typer.run(main)
