"""
ETL: Bronze → Silver
Transforma el archivo .xlsm de la herramienta de seguimiento Canguro
(o cualquier Excel compatible) desde formato ancho (una fila = un paciente)
a formato largo en hecho_medicion_paciente (una fila = una medición).

Flujo:
  1. Cargar el Excel y mapear columnas → UUID de variable (mapeo inteligente)
  2. Upsert fundacion y paciente
  3. Melt (unpivot) del DataFrame: ancho → largo
  4. Insert masivo en hecho_medicion_paciente con UPSERT idempotente

Columnas del Excel que se tratan como identificadores (no mediciones):
  LLAVE, COD_PAIS, ID_CANGURO, ID_NACIONAL,
  NOMBRE_INSTITUCION_*, NUMERO_INSTITUCION_*, NIVEL_INSTITUCION_*,
  NACIO_INSTITUCION_*, REG_SEC_*, MES_NACIMIENTO, ANIO_NACIMIENTO,
  NOMBRE_INSTITUCION_REFERIDA, NUMERO_INSTITUCION_REFERIDA

Columnas de fecha que se convierten al formato DDMMYYYY de la dimensión fecha:
  FECHA_NACIMIENTO, FECHA_MUERTE, FECHA_SALIDA_SALA_PARTOS,
  FECHA_ENTRADA_ALOJAMIENTO_CONJUNTO, FECHA_ENTRADA_URN,
  FECHA_ALTA_URN, FECHA_SALIDA_HOSPITAL_AMBULATORIO,
  FECHA_ADMISION_PMC_AMBULATORIO
"""

from datetime import datetime, date
from typing import Optional

import pandas as pd

from app.core.database import supabase_client


# ─────────────────────────────────────────────
# Constantes
# ─────────────────────────────────────────────

# Columnas que son identidad del paciente/registro, no mediciones clínicas
COLS_IDENTIDAD = {
    "LLAVE", "COD_PAIS", "ID_CANGURO", "ID_NACIONAL",
    "NOMBRE_INSTITUCION_ACTUAL", "NUMERO_INSTITUCION_ACTUAL",
    "NIVEL_INSTITUCION_ACTUAL", "NACIO_INSTITUCION_ACTUAL",
    "NACIO_INSTITUCIÓN_ACTUAL",  # variante con tilde del DB_TOTAL
    "NOMBRE_INSTITUCION_NACIMIENTO", "NUMERO_INSTITUCION_NACIMIENTO",
    "NIVEL_INSTITUCION_NACIMIENTO",
    "NOMBRE_INSTITUCION_REFERIDA", "NUMERO_INSTITUCION_REFERIDA",
    "MES_NACIMIENTO", "ANIO_NACIMIENTO", "AÑO_NACIMIENTO",
    "REG_SEC_CLIN", "REG_SEC_FALLECIDO", "REG_SEC_SALA",
    "REG_SEC_ALOJ", "REG_SEC_URN",
    "REG_SEC_AMBU_BASE", "REG_SEC_AMBU_40S", "REG_SEC_AMBU_3M",
    "REG_SEC_AMBU_6M", "REG_SEC_AMBU_9M", "REG_SEC_AMBU_12M",
}

# Columnas de fecha a convertir a DDMMYYYY para id_fecha
COLS_FECHA = {
    "FECHA_NACIMIENTO", "FECHA_MUERTE",
    "FECHA_SALIDA_SALA_PARTOS", "FECHA_ENTRADA_ALOJAMIENTO_CONJUNTO",
    "FECHA_ENTRADA_URN", "FECHA_ALTA_URN",
    "FECHA_SALIDA_HOSPITAL_AMBULATORIO", "FECHA_ADMISION_PMC_AMBULATORIO",
}


# ─────────────────────────────────────────────
# Carga del mapa variable nombre_bd → UUID
# ─────────────────────────────────────────────

def _normalizar(nombre: str) -> str:
    """Replica la normalización de dynamic_validator.py.
    Quita el prefijo antes del primer '_', elimina todos los '_',
    convierte a mayúsculas.
    Ej: 'PA_PesoNacer' → 'PESONACER', 'PESO_NACER' → 'PESONACER'
    """
    sin_prefijo = nombre.split("_", 1)[-1]
    return sin_prefijo.replace("_", "").upper()


def cargar_mapa_variables() -> dict[str, str]:
    """
    Devuelve {nombre_normalizado: id_variable_uuid} para todas
    las variables activas en Supabase.
    Se llama una vez por ejecución del ETL.
    """
    response = supabase_client.table("variable").select("id, nombre_bd").execute()
    mapa = {}
    for v in (response.data or []):
        norm = _normalizar(v["nombre_bd"])
        mapa[norm] = v["id"]
    return mapa


# ─────────────────────────────────────────────
# Upsert de dimensiones
# ─────────────────────────────────────────────

def upsert_fundacion(nombre: str, codigo_pais: Optional[str] = None) -> str:
    """Devuelve el UUID de la fundación, creándola si no existe."""
    res = supabase_client.table("fundacion").select("id").eq("nombre", nombre).execute()
    if res.data:
        return res.data[0]["id"]
    ins = supabase_client.table("fundacion").insert({
        "nombre": nombre,
        "codigo_pais": codigo_pais,
    }).execute()
    return ins.data[0]["id"]


def upsert_paciente(id_canguro: str, id_nacional: Optional[str],
                    cod_pais: Optional[str], id_fundacion: str) -> str:
    """Devuelve el UUID del paciente, creándolo si no existe.
    Si existe, no actualiza (el id_canguro es inmutable).
    """
    res = supabase_client.table("paciente").select("id").eq("id_canguro", str(id_canguro)).execute()
    if res.data:
        return res.data[0]["id"]
    ins = supabase_client.table("paciente").insert({
        "id_canguro": str(id_canguro),
        "id_nacional": str(id_nacional) if id_nacional else None,
        "cod_pais": str(cod_pais) if cod_pais else None,
        "id_fundacion": id_fundacion,
    }).execute()
    return ins.data[0]["id"]


# ─────────────────────────────────────────────
# Conversión de fechas
# ─────────────────────────────────────────────

def _fecha_a_id(valor) -> Optional[str]:
    """Convierte un valor de celda Excel con fecha al formato DDMMYYYY."""
    if valor is None or (isinstance(valor, float) and str(valor) == "nan"):
        return None
    if isinstance(valor, (datetime, date)):
        return valor.strftime("%d%m%Y")
    try:
        dt = pd.to_datetime(valor, dayfirst=True, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.strftime("%d%m%Y")
    except Exception:
        return None


# ─────────────────────────────────────────────
# Función principal del ETL
# ─────────────────────────────────────────────

def ejecutar_etl(
    ruta_excel: str,
    nombre_hoja: str,
    nombre_fundacion: str,
    id_upload: Optional[str] = None,
    batch_size: int = 500,
) -> dict:
    """
    Carga el Excel indicado, transforma a formato largo e inserta en
    hecho_medicion_paciente.

    Args:
        ruta_excel:       Ruta al .xlsx / .xlsm
        nombre_hoja:      Nombre de la hoja a procesar ('DB_PACIENTES' o 'DB_SEGUIMIENTO')
        nombre_fundacion: Nombre de la fundación que reporta
        id_upload:        UUID del upload_session de Bronze (para trazabilidad)
        batch_size:       Tamaño del lote para inserciones

    Returns:
        {"insertados": int, "omitidos_sin_variable": int, "errores": list}
    """
    # 1. Cargar Excel
    df = pd.read_excel(ruta_excel, sheet_name=nombre_hoja)
    df.columns = [str(c).strip().upper() for c in df.columns]

    # Verificar columna de identidad clave
    if "ID_CANGURO" not in df.columns:
        raise ValueError(f"La hoja '{nombre_hoja}' no tiene columna ID_CANGURO")

    # 2. Cargar mapa de variables EFETI
    mapa_variables = cargar_mapa_variables()

    # 3. Identificar columnas de medición (todo lo que no es identidad)
    cols_medicion = [
        c for c in df.columns
        if c not in COLS_IDENTIDAD and c not in COLS_FECHA
    ]

    # 4. Resolver fundación (una por archivo)
    codigo_pais = str(df["COD_PAIS"].iloc[0]) if "COD_PAIS" in df.columns and not df["COD_PAIS"].isna().all() else None
    id_fundacion = upsert_fundacion(nombre_fundacion, codigo_pais)

    stats = {"insertados": 0, "omitidos_sin_variable": 0, "errores": []}
    lote: list[dict] = []

    for _, fila in df.iterrows():
        id_canguro = fila.get("ID_CANGURO")
        if pd.isna(id_canguro) or id_canguro is None:
            continue  # fila vacía

        # 5. Upsert paciente
        try:
            id_paciente = upsert_paciente(
                id_canguro=id_canguro,
                id_nacional=fila.get("ID_NACIONAL"),
                cod_pais=fila.get("COD_PAIS"),
                id_fundacion=id_fundacion,
            )
        except Exception as e:
            stats["errores"].append({"id_canguro": id_canguro, "error": str(e)})
            continue

        # 6. Por cada columna de medición → una fila en hecho_medicion_paciente
        for col in cols_medicion:
            valor_raw = fila.get(col)

            # Saltar celdas vacías
            if valor_raw is None or (isinstance(valor_raw, float) and pd.isna(valor_raw)):
                continue

            # Resolver id_variable via mapeo inteligente
            norm_col = _normalizar(col)
            id_variable = mapa_variables.get(norm_col)
            if not id_variable:
                stats["omitidos_sin_variable"] += 1
                continue

            # Resolver id_fecha si la columna tiene una fecha asociada
            id_fecha = None
            if col in COLS_FECHA:
                id_fecha = _fecha_a_id(valor_raw)

            lote.append({
                "id_paciente":  id_paciente,
                "id_variable":  id_variable,
                "id_fecha":     id_fecha,
                "id_fase":      None,      # fase: futuro — inferir del nombre de columna
                "id_episodio":  None,      # episodio: futuro
                "valor":        str(valor_raw),
                "id_upload":    id_upload,
                "id_fundacion": id_fundacion,
            })

            # Flush del lote
            if len(lote) >= batch_size:
                _flush(lote, stats)
                lote = []

    # Flush final
    if lote:
        _flush(lote, stats)

    return stats


def _flush(lote: list[dict], stats: dict) -> None:
    """Inserta el lote en Supabase con UPSERT idempotente."""
    try:
        supabase_client.table("hecho_medicion_paciente").upsert(
            lote,
            on_conflict="id_paciente,id_variable,id_fecha",
            ignore_duplicates=True,
        ).execute()
        stats["insertados"] += len(lote)
    except Exception as e:
        stats["errores"].append({"batch_error": str(e), "filas": len(lote)})


# ─────────────────────────────────────────────
# Entrada principal usada por el pipeline automático
# Lee desde bronze_raw_clinical_data (JSONB) — no necesita el archivo original
# ─────────────────────────────────────────────

def ejecutar_etl_desde_bronze(
    upload_id: str,
    nombre_fundacion: str = "Fundación Canguro",
    batch_size: int = 500,
) -> dict:
    """
    Versión del ETL que lee los datos desde bronze_raw_clinical_data.raw_payload
    (JSONB ya guardado en Supabase). No necesita el archivo Excel original.

    Llamada automáticamente por promote_to_bronze via BackgroundTask,
    y también disponible como endpoint manual POST /api/v1/etl/run.
    """
    # 1. Leer raw_payload de Bronze
    res = supabase_client.table("bronze_raw_clinical_data") \
        .select("raw_payload, filename") \
        .eq("upload_id", upload_id) \
        .execute()

    if not res.data:
        raise ValueError(f"No hay datos en Bronze para upload_id={upload_id}")

    raw_payload = res.data[0]["raw_payload"]
    if not raw_payload:
        raise ValueError("raw_payload está vacío")

    # 2. Reconstruir DataFrame desde JSONB
    df = pd.DataFrame(raw_payload)
    df.columns = [str(c).strip().upper() for c in df.columns]

    if "ID_CANGURO" not in df.columns:
        raise ValueError("El payload de Bronze no contiene columna ID_CANGURO")

    # 3. Reutilizar la lógica central del ETL
    mapa_variables = cargar_mapa_variables()

    cols_medicion = [
        c for c in df.columns
        if c not in COLS_IDENTIDAD and c not in COLS_FECHA
    ]

    codigo_pais = str(df["COD_PAIS"].iloc[0]) if "COD_PAIS" in df.columns and not df["COD_PAIS"].isna().all() else None
    id_fundacion = upsert_fundacion(nombre_fundacion, codigo_pais)

    stats = {"upload_id": upload_id, "insertados": 0, "omitidos_sin_variable": 0, "errores": []}
    lote: list[dict] = []

    for _, fila in df.iterrows():
        id_canguro = fila.get("ID_CANGURO")
        if not id_canguro or (isinstance(id_canguro, float) and pd.isna(id_canguro)):
            continue

        try:
            id_paciente = upsert_paciente(
                id_canguro=id_canguro,
                id_nacional=fila.get("ID_NACIONAL"),
                cod_pais=fila.get("COD_PAIS"),
                id_fundacion=id_fundacion,
            )
        except Exception as e:
            stats["errores"].append({"id_canguro": id_canguro, "error": str(e)})
            continue

        for col in cols_medicion:
            valor_raw = fila.get(col)
            if valor_raw is None or (isinstance(valor_raw, float) and pd.isna(valor_raw)):
                continue

            id_variable = mapa_variables.get(_normalizar(col))
            if not id_variable:
                stats["omitidos_sin_variable"] += 1
                continue

            id_fecha = _fecha_a_id(valor_raw) if col in COLS_FECHA else None

            lote.append({
                "id_paciente":  id_paciente,
                "id_variable":  id_variable,
                "id_fecha":     id_fecha,
                "id_fase":      None,
                "id_episodio":  None,
                "valor":        str(valor_raw),
                "id_upload":    upload_id,
                "id_fundacion": id_fundacion,
            })

            if len(lote) >= batch_size:
                _flush(lote, stats)
                lote = []

    if lote:
        _flush(lote, stats)

    # Marcar el upload como procesado en Silver
    supabase_client.table("upload_sessions").update(
        {"status": "silver_processed"}
    ).eq("id", upload_id).execute()

    return stats
