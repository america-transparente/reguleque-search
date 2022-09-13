from dask.dataframe.core import DataFrame
from dask.dataframe.io.csv import read_csv

from datetime import datetime

from pathlib import Path

def load_file(filepath: Path) -> DataFrame:
    df = read_csv(
        filepath,
        sep=";",
        skip_blank_lines=True,
        na_filter=True,
        na_values="Indefinido",
        encoding="ISO-8859-1",
    ).dropna(how="all")

    # Fill NA properly
    for col in df:
        # get dtype for column
        dt = df[col].dtype
        # check if it is a number
        if dt == int or dt == float:
            df[col] = df[col].fillna(0)
        else:
            df[col] = df[col].fillna("")

    # Rename columns to fit schema
    df = df.rename(columns = {
        "entidad": "nombre_organismo",
        "anyo": "año",
        "descripcion_funcion": "tipo_cargo",
        "region": "región",
        "remuneracion_bruta": "remuneración_bruta_mensual",
        "remuliquida_mensual": "remuneración_líquida_mensual",
        "pago_hextra_diurnas": "pago_horas_diurnas",
        "horas_extra_diurnas": "horas_diurnas",
        "pago_hextra_nocturnas": "pago_horas_nocturnas",
        "horas_extra_nocturnas": "hornas_nocturnas",
        "pago_hextra_festivas": "pago_horas_festivas",
        "horas_extra_festivas": "horas_festivas",
        "tipo_calificacion_profesional": "tipo_calificación_profesional",
        "pago_mensuales": "pago_mensual",
        "viaticos": "viáticos",
        "descripcion_funcion": "descripción_función",
        "fecha_termino": "fecha_término"
    })

    # Convert remuneración_bruta_mensual & remuneración_líquida_mensual to floats
    df["remuneración_bruta_mensual"] = df["remuneración_bruta_mensual"].astype(float)
    df["remuneración_líquida_mensual"] = df["remuneración_líquida_mensual"].astype(float)

    # If the column "observaciones" exist, exclude out the rows which
    # mention "funcionario de prueba"
    if "observaciones" in df.columns:
        df = df[~df["observaciones"].str.contains("funcionario de prueba", case=False)]

    # Add column "tipo_contrato" with the first part of the filename
    df["tipo_contrato"] = filepath.name.split("_")[0]

    # Add column "fecha_publicación" with the current date
    df["fecha_publicación"] = datetime.now().strftime("%Y-%m-%d")

    # Add column id based on the filename and the index
    df["id"] = df.index + 1
    df["id"] = df["id"].astype(str)
    df["id"] = filepath.name.split(".")[0] + "_" + df["id"]

    return df
