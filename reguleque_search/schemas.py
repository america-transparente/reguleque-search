from collections import OrderedDict

REFERENCE_TYPES = {
    "id": str, # ❌
    "tipo_contrato": str,
    "nombre_organismo": str, # ✅
    "código_organismo": str,
    "fecha_publicación": str,
    "año": str, # ✅
    "mes": str, # ✅
    "tipo_estamento": str,
    "nombre": str, # ✅
    "tipo_calificación_profesional": str, # ✅
    "tipo_cargo": str, # ✅
    "región": str, # ✅
    "asignaciones": str, # ✅
    "unidad_monetaria": str, # ✅x
    "remuneración_bruta_mensual": float, # ✅
    "remuneración_líquida_mensual": float, # ✅
    "horas_diurnas": str, # ✅
    "horas_nocturnas": str, # ✅
    "horas_festivas": str, # ✅
    "fecha_ingreso": str, # ✅
    "fecha_término": str, # ✅
    "observaciones": str, # ✅
    "enlace": str, # ✅
    "viáticos": str, # ✅
    "pago_mensual": str,
}

# codigo_del_trabajo schema:
# entidad;anyo;mes;nombre;descripcion_funcion;grado_eus;tipo_calificacion_profesional;region;asignaciones;unidad_monetaria;remuneracion_bruta;remuliquida_mensual;pago_hextra_diurnas;horas_extra_diurnas;pago_hextra_nocturnas;horas_extra_nocturnas;pago_hextra_festivas;horas_extra_festivas;fecha_ingreso;fecha_termino;observaciones;viaticos;enlace


SCHEMAS: dict[str, list[tuple[str, type]]] = {
    "codigo_del_trabajo": [
        ("nombre_organismo", str),
        ("año", str),
        ("mes", str),
        ("nombre", str),
        ("tipo_cargo", str),
        ("grado_eus", str),
        ("tipo_calificación_profesional", str),
        ("región", str),
        ("asignaciones", str),
        ("unidad_monetaria", str),
        ("remuneración_bruta_mensual", float),
        ("remuneración_líquida_mensual", float),
        ("pagos_horas_diurnas", str),
        ("horas_diurnas", str),
        ("pagos_horas_nocturnas", str),
        ("horas_nocturnas", str),
        ("pago_horas_festivas", str),
        ("horas_festivas", str),
        ("fecha_ingreso", str),
        ("fecha_termino", str),
        ("observaciones", str),
        ("viáticos", str),
        ("enlace", str)
    ],
    "contrata": [
        ("nombre_organismo", str),
        ("año", str),
        ("mes", str),
        ("tipo_estamento", str),
        ("nombre", str),
        ("grado_eus", str),
        ("tipo_calificación_profesional", str),
        ("descripcion_función", str),
        ("región", str),
        ("asignaciones", str),
        ("unidad_monetaria", str),
        ("remuneración_bruta_mensual", float),
        ("remuneración_líquida_mensual", float),
        ("pagos_horas_diurnas", str),
        ("horas_diurnas", str),
        ("pagos_horas_nocturnas", str),
        ("horas_nocturnas", str),
        ("pago_horas_festivas", str),
        ("horas_festivas", str),
        ("fecha_ingreso", str),
        ("fecha_termino", str),
        ("observaciones", str),
        ("enlace", str),
        ("viáticos", str),
    ],
    "honorarios": [
        ("nombre_organismo", str),
        ("año", str),
        ("mes", str),
        ("nombre", str),
        ("tipo_calificación_profesional", str),
        ("descripcion_función", str),
        ("región", str),
        ("asignaciones", str),
        ("unidad_monetaria", str),
        ("remuneración_bruta_mensual", float),
        ("remuneración_líquida_mensual", float),
        ("pagos_horas_diurnas", str),
        ("horas_diurnas", str),
        ("pagos_horas_nocturnas", str),
        ("horas_nocturnas", str),
        ("pago_horas_festivas", str),
        ("horas_festivas", str),
        ("fecha_ingreso", str),
        ("fecha_termino", str),
        ("observaciones", str),
        ("enlace", str),
        ("viáticos", str),
    ],
    "planta": []
}
