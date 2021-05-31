import os
import pandas as pd
import typesense as ts

client = ts.Client(
    {
        "api_key": os.getenv("TYPESENSE_API_KEY") or input("TypeSense Admin API Key: "),
        "nodes": [{"host": os.getenv("TYPESENSE_HOST") or "api.reguleque.cl", "port": "443", "protocol": "https"}],
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
    "name": "revenue_entry",
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
        entries["grado_eus"] = entries["grado_eus"].astype(int)
        # entries["año"] = entries["año"].apply(lambda x: int(x) if x == x else "")
        entries = entries.to_dict(orient="records")

    print(entries[0])
    for entry in entries:
        client.collections["revenue_entry"].documents.create(entry)

    # client.collections["revenue_entry"].documents.import_(entries, {'action': 'create'})
