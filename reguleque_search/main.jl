using JuliaDB, DelimitedFiles

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

t = loadtable(glob("data/in/*.csv"), delim=';', colnames=COLUMNS, header_exists=false)
