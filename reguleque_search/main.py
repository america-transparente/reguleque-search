import os
import pandas as pd
import typesense as ts

client = ts.Client(
    {
        "api_key": "xd",
        "nodes": [{"host": "localhost", "port": "8108", "protocol": "http"}],
    }
)

datapath = os.path.relpath("data/in", os.getcwd())
filepaths = [os.path.join(datapath, fn) for fn in next(os.walk(datapath))[2]]

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
    "remuneración_bruta",
    "remuneración_líquida_mensual",
    "diurnas",
    "nocturnas",
    "festivas",
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
    "año": float,
    "mes": str,
    "tipo_estamento": str,
    "nombre": str,
    "grado_eus": str,
    "tipo_calificación_profesional": str,
    "tipo_cargo": str,
    "región": str,
    "asignaciones": str,
    "unidad_monetaria": str,
    "remuneración_bruta": float,
    "remuneración_líquida_mensual": float,
    "diurnas": str,
    "nocturnas": str,
    "festivas": str,
    "fecha_ingreso": str,
    "fecha_término": str,
    "observaciones": str,
    "enlace": str,
    "viáticos": str,
}


REVENUE_SCHEMA = {
    "name": "revenue_entry",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "tipo_contrato", "type": "string", "facet": True},
        {"name": "path", "type": "string"},
        {"name": "nombre_organismo", "type": "string", "facet": True},
        {"name": "código_organismo", "type": "string"},
        {"name": "fecha_publicación", "type": "string"},
        {"name": "año", "type": "int32", "facet": True},
        {"name": "mes", "type": "string"},
        {"name": "tipo_estamento", "type": "string", "facet": True},
        {"name": "nombre", "type": "string"},
        {"name": "grado_eus", "type": "string"},
        {"name": "tipo_cargo", "type": "string"},
        {"name": "región", "type": "string"},
        {"name": "asignaciones", "type": "string"},
        {"name": "unidad_monetaria", "type": "string"},
        {"name": "remuneración_bruta", "type": "float"},
        {"name": "remuneración_líquida_mensual", "type": "float"},
        {"name": "diurnas", "type": "string"},
        {"name": "nocturnas", "type": "string"},
        {"name": "festivas", "type": "string"},
        {"name": "fecha_ingreso", "type": "string"},
        {"name": "fecha_término", "type": "string"},
        {"name": "observaciones", "type": "string"},
        {"name": "enlace", "type": "string"},
        {"name": "viáticos", "type": "string"},
    ],
    "default_sorting_field": "remuneración_bruta",
}


# Drop pre-existing collection if any
try:
    client.collections["revenue_entry"].delete()
except Exception:
    pass

# Create collection with the manual schema
client.collections.create(REVENUE_SCHEMA)

# Load all files
for filepath in filepaths:
    with open(filepath, encoding="ISO-8859-1") as f:
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
                # parse_dates=["fecha_publicación", "fecha_ingreso", "fecha_término"],
                dayfirst=True,
                # infer_datetime_format=True,
                encoding="ISO-8859-1",
                index_col="id",
            )
            .dropna(axis=0, how="all")
            .fillna("")
        )
        entries["año"] = entries["año"].apply(lambda x: int(x) if x == x else "")
        entries = entries.to_dict(orient="records")

    print(entries[0])
    for entry in entries:
        client.collections["revenue_entry"].documents.create(entry)

    print(len(entries))
    # client.collections["revenue_entry"].documents.import_(entries, {'action': 'create'})
