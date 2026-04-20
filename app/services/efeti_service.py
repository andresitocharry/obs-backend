"""
Servicio de administración del modelo EFETI.
Implementa SCD Tipo 2 (Kimball) sin los workarounds de la GUI heredada:
  - No modifica campos históricos (como `ultimo`) al cerrar versiones
  - No hace mass-update de punteros `variable_activa`
  - Solo: UPDATE antiguo (activo=False, fecha_fin=hoy) + INSERT nuevo
"""

from datetime import date
from fastapi import HTTPException, status
from app.core.database import supabase_client
from app.schemas.efeti import (
    EventoCreate, EventoUpdate,
    FaseCreate, FaseUpdate,
    EpisodioCreate, EpisodioUpdate,
    ConvertirCambianteRequest, AgregarHistoriaRequest,
)


def _hoy() -> str:
    return date.today().isoformat()


def _scd2_cerrar(tabla: str, id_registro: str) -> None:
    """Cierra una versión histórica: activo=False, fecha_fin=hoy."""
    result = supabase_client.table(tabla).update({
        "activo": False,
        "fecha_fin": _hoy()
    }).eq("id", id_registro).execute()

    if not result.data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Registro '{id_registro}' no encontrado en tabla '{tabla}'"
        )


# ─────────────────────────────────────────────
# EVENTO
# ─────────────────────────────────────────────

def listar_eventos(solo_activos: bool = True) -> list:
    q = supabase_client.table("evento").select(
        "id, nombre, descripcion, id_variable_fecha, fecha_inicio, fecha_fin, activo"
    )
    if solo_activos:
        q = q.eq("activo", True)
    return q.order("nombre").execute().data or []


def obtener_evento(evento_id: str) -> dict:
    result = supabase_client.table("evento").select("*").eq("id", evento_id).execute()
    if not result.data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Evento no encontrado")
    return result.data[0]


def crear_evento(payload: EventoCreate) -> dict:
    data = {
        "nombre": payload.nombre,
        "descripcion": payload.descripcion,
        "id_variable_fecha": payload.id_variable_fecha,
        "fecha_inicio": _hoy(),
        "fecha_fin": "9999-12-31",
        "activo": True,
    }
    result = supabase_client.table("evento").insert(data).execute()
    if not result.data:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error al crear evento")
    return result.data[0]


def actualizar_evento_scd2(evento_id: str, payload: EventoUpdate) -> dict:
    """SCD2: cierra versión antigua e inserta nueva. No toca otros campos históricos."""
    obtener_evento(evento_id)  # valida existencia

    _scd2_cerrar("evento", evento_id)

    data_nuevo = {
        "nombre": payload.nombre,
        "descripcion": payload.descripcion,
        "id_variable_fecha": payload.id_variable_fecha,
        "fecha_inicio": _hoy(),
        "fecha_fin": "9999-12-31",
        "activo": True,
    }
    result = supabase_client.table("evento").insert(data_nuevo).execute()
    if not result.data:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error al insertar nueva versión de evento")
    return result.data[0]


# ─────────────────────────────────────────────
# FASE
# Los eventos de inicio/fin se almacenan en la tabla puente fase_evento.
# ─────────────────────────────────────────────

def listar_fases(solo_activos: bool = True) -> list:
    q = supabase_client.table("fase").select(
        "id, nombre_analisis, nombre_bd, descripcion, num_fase, ultimo, fecha_inicio, fecha_fin, activo"
    )
    if solo_activos:
        q = q.eq("activo", True)
    return q.order("num_fase").execute().data or []


def obtener_fase(fase_id: str) -> dict:
    result = supabase_client.table("fase").select("*").eq("id", fase_id).execute()
    if not result.data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Fase no encontrada")
    return result.data[0]


def crear_fase(payload: FaseCreate) -> dict:
    data_fase = {
        "nombre_analisis": payload.nombre_analisis,
        "nombre_bd": payload.nombre_bd,
        "descripcion": payload.descripcion,
        "num_fase": payload.num_fase,
        "ultimo": payload.ultimo,
        "fecha_inicio": _hoy(),
        "fecha_fin": "9999-12-31",
        "activo": True,
    }
    result = supabase_client.table("fase").insert(data_fase).execute()
    if not result.data:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error al crear fase")

    id_fase = result.data[0]["id"]

    supabase_client.table("fase_evento").insert({
        "id_fase": id_fase,
        "id_evento_inicio": payload.id_evento_inicio,
        "id_evento_fin": payload.id_evento_fin,
    }).execute()

    return result.data[0]


def actualizar_fase_scd2(fase_id: str, payload: FaseUpdate) -> dict:
    """
    SCD2 para Fase.
    - Cierra versión antigua (activo=False, fecha_fin=hoy) SIN modificar su campo `ultimo`
      (las versiones históricas conservan los atributos que tenían cuando estuvieron vigentes).
    - Inserta nueva versión con el `ultimo` que venga en el payload.
    - Crea nueva entrada en fase_evento para la nueva versión.
    """
    obtener_fase(fase_id)  # valida existencia

    _scd2_cerrar("fase", fase_id)

    data_nueva_fase = {
        "nombre_analisis": payload.nombre_analisis,
        "nombre_bd": payload.nombre_bd,
        "descripcion": payload.descripcion,
        "num_fase": payload.num_fase,
        "ultimo": payload.ultimo,
        "fecha_inicio": _hoy(),
        "fecha_fin": "9999-12-31",
        "activo": True,
    }
    result = supabase_client.table("fase").insert(data_nueva_fase).execute()
    if not result.data:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error al insertar nueva versión de fase")

    id_nueva_fase = result.data[0]["id"]

    supabase_client.table("fase_evento").insert({
        "id_fase": id_nueva_fase,
        "id_evento_inicio": payload.id_evento_inicio,
        "id_evento_fin": payload.id_evento_fin,
    }).execute()

    return result.data[0]


# ─────────────────────────────────────────────
# EPISODIO
# ─────────────────────────────────────────────

def listar_episodios(solo_activos: bool = True) -> list:
    q = supabase_client.table("episodio").select(
        "id, nombre_analisis, nombre_bd, descripcion, id_evento_inicio, id_evento_fin, fecha_inicio, fecha_fin, activo"
    )
    if solo_activos:
        q = q.eq("activo", True)
    return q.order("nombre_analisis").execute().data or []


def obtener_episodio(episodio_id: str) -> dict:
    result = supabase_client.table("episodio").select("*").eq("id", episodio_id).execute()
    if not result.data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Episodio no encontrado")
    return result.data[0]


def crear_episodio(payload: EpisodioCreate) -> dict:
    data = {
        "nombre_analisis": payload.nombre_analisis,
        "nombre_bd": payload.nombre_bd,
        "descripcion": payload.descripcion,
        "id_evento_inicio": payload.id_evento_inicio,
        "id_evento_fin": payload.id_evento_fin,
        "fecha_inicio": _hoy(),
        "fecha_fin": "9999-12-31",
        "activo": True,
    }
    result = supabase_client.table("episodio").insert(data).execute()
    if not result.data:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error al crear episodio")
    return result.data[0]


def actualizar_episodio_scd2(episodio_id: str, payload: EpisodioUpdate) -> dict:
    """
    SCD2 para Episodio.
    Los id_evento_inicio/fin capturan el UUID del evento válido en este momento histórico.
    No se fuerza que apunten a activo=True (eso es responsabilidad del frontend al poblar dropdowns).
    """
    obtener_episodio(episodio_id)  # valida existencia

    _scd2_cerrar("episodio", episodio_id)

    data_nuevo = {
        "nombre_analisis": payload.nombre_analisis,
        "nombre_bd": payload.nombre_bd,
        "descripcion": payload.descripcion,
        "id_evento_inicio": payload.id_evento_inicio,
        "id_evento_fin": payload.id_evento_fin,
        "fecha_inicio": _hoy(),
        "fecha_fin": "9999-12-31",
        "activo": True,
    }
    result = supabase_client.table("episodio").insert(data_nuevo).execute()
    if not result.data:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error al insertar nueva versión de episodio")
    return result.data[0]


# ─────────────────────────────────────────────
# VARIABLE — listados de apoyo para dropdowns
# ─────────────────────────────────────────────

def listar_variables(tipo: str = "all") -> list:
    """
    tipo: 'basica' | 'cambiante' | 'fecha' | 'all'
    """
    q = supabase_client.table("variable").select(
        "id, nombre_analisis, nombre_bd, tipo_dato, basica"
    )
    if tipo == "basica":
        q = q.eq("basica", True)
    elif tipo == "cambiante":
        q = q.eq("basica", False)
    elif tipo == "fecha":
        q = q.ilike("tipo_dato", "DATE%")

    return q.order("nombre_analisis").execute().data or []


def listar_historias_variable(variable_id: str) -> list:
    result = supabase_client.table("variable_cambiante").select(
        "id_historia, id, variable_fecha_inicio, variable_fecha_fin, activa"
    ).eq("id", variable_id).order("id_historia").execute()
    return result.data or []


def convertir_variable_a_cambiante(variable_id: str, payload: ConvertirCambianteRequest) -> dict:
    """
    Convierte una variable básica (basica=True) a cambiante.
    Pasos:
    1. Verifica que la variable existe y es básica.
    2. Marca variable.basica = False.
    3. Inserta primer registro en variable_cambiante con activa=True.
    No actualiza el puntero variable_activa en otras filas (workaround eliminado).
    """
    var_result = supabase_client.table("variable").select("id, basica").eq("id", variable_id).execute()
    if not var_result.data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Variable no encontrada")

    variable = var_result.data[0]
    if not variable.get("basica", True):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="La variable ya es cambiante. Usa 'agregar-historia' para añadir una nueva versión."
        )

    supabase_client.table("variable").update({"basica": False}).eq("id", variable_id).execute()

    result = supabase_client.table("variable_cambiante").insert({
        "id": variable_id,
        "variable_fecha_inicio": payload.id_variable_fecha_inicio,
        "variable_fecha_fin": payload.id_variable_fecha_fin,
        "activa": True,
    }).execute()

    if not result.data:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error al crear registro de variable cambiante")
    return result.data[0]


def agregar_historia_variable(variable_id: str, payload: AgregarHistoriaRequest) -> dict:
    """
    Agrega una nueva versión histórica a una variable cambiante (SCD2).
    1. Desactiva todas las historias previas (activa=False).
    2. Inserta nueva historia con activa=True.
    No hace mass-update de variable_activa (workaround eliminado, no es estándar Kimball).
    """
    historias = supabase_client.table("variable_cambiante").select("id_historia").eq("id", variable_id).execute()
    if not historias.data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Variable no encontrada o no es cambiante. Usa 'convertir-cambiante' primero."
        )

    supabase_client.table("variable_cambiante").update({"activa": False}).eq("id", variable_id).execute()

    result = supabase_client.table("variable_cambiante").insert({
        "id": variable_id,
        "variable_fecha_inicio": payload.id_variable_fecha_inicio,
        "variable_fecha_fin": payload.id_variable_fecha_fin,
        "activa": True,
    }).execute()

    if not result.data:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error al insertar nueva historia")
    return result.data[0]
