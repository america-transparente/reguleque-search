from dask.dataframe.core import DataFrame
from dask.dataframe.io.csv import read_csv

from datetime import datetime

from pathlib import Path


def load_file(filepath: Path) -> DataFrame:
    ENFORCED_DTYPES = {
        "grado_eus": str,
        "fecha_inicio": str,
        "fecha_termino": str,
        "pago_hextra_diurnas": "float64",
        "pago_hextra_festivas": "float64",
        "pago_hextra_nocturnas": "float64",
        "remuliquida_mensual": "float64",
        "viaticos": str,
        "asignaciones": str,
        "observaciones": str
    }
    df = read_csv(
        filepath,
        sep=";",
        skip_blank_lines=True,
        na_filter=True,
        na_values="Indefinido",
        encoding="ISO-8859-1",
        dtype=ENFORCED_DTYPES,
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

    # Lowercase columns
    df.columns = [col.lower() for col in df.columns]

    # Rename columns to fit schema
    df = df.rename(
        columns={
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
            "fecha_termino": "fecha_término",
            "organismo_nombre": "nombre_organismo",
            "tipo_calificacionp": "tipo_calificación_profesional",
            "Tipo Unidad monetaria": "unidad_monetaria",
            "remuneracionbruta": "remuneración_bruta_mensual",
        }
    )

    # if remuneración_bruta_mensual not present, raise a warning and print the path
    if "remuneración_bruta_mensual" not in df.columns:
        print(f"WARNING: remuneración_bruta_mensual not present in {filepath}")


    # Convert remuneración_bruta_mensual & remuneración_líquida_mensual to floats
    df["remuneración_bruta_mensual"] = df["remuneración_bruta_mensual"].astype(float)
    df["remuneración_líquida_mensual"] = df["remuneración_líquida_mensual"].astype(
        float
    )

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
    # current year + _ + month _ + first part of filename + _id
    df["id"] = datetime.now().strftime("%Y") + "_" + datetime.now().strftime("%m") + "_" + filepath.name.split("_")[0] + "_" + df["id"]

    return df
