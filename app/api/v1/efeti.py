from fastapi import APIRouter, Depends, HTTPException, status, Query
from typing import List, Optional

from app.schemas.efeti import (
    Evento, EventoCreate, EventoUpdate,
    Fase, FaseCreate, FaseUpdate,
    Episodio, EpisodioCreate, EpisodioUpdate,
    VariableResumen, HistoriaVariableCambiante,
    ConvertirCambianteRequest, AgregarHistoriaRequest,
)
from app.services import efeti_service
from app.core.auth import get_current_user


def get_current_admin(user_data: dict = Depends(get_current_user)):
    if user_data.get("role") != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Se requieren permisos de administrador"
        )
    return user_data


router = APIRouter(prefix="/efeti", tags=["EFETI — Administración de Metadatos"])


# ─────────────────────────────────────────────
# EVENTOS
# ─────────────────────────────────────────────

@router.get("/eventos", response_model=List[Evento])
def get_eventos(
    solo_activos: bool = Query(True, description="True = solo versiones vigentes (para dropdowns)"),
    _admin: dict = Depends(get_current_admin),
):
    return efeti_service.listar_eventos(solo_activos)


@router.get("/eventos/{evento_id}", response_model=Evento)
def get_evento(evento_id: str, _admin: dict = Depends(get_current_admin)):
    return efeti_service.obtener_evento(evento_id)


@router.post("/eventos", response_model=Evento, status_code=status.HTTP_201_CREATED)
def post_evento(payload: EventoCreate, _admin: dict = Depends(get_current_admin)):
    return efeti_service.crear_evento(payload)


@router.put("/eventos/{evento_id}", response_model=Evento)
def put_evento(evento_id: str, payload: EventoUpdate, _admin: dict = Depends(get_current_admin)):
    """
    SCD2: cierra la versión actual del evento e inserta una nueva versión vigente.
    La versión antigua queda con activo=False y fecha_fin=hoy.
    """
    return efeti_service.actualizar_evento_scd2(evento_id, payload)


# ─────────────────────────────────────────────
# FASES
# ─────────────────────────────────────────────

@router.get("/fases", response_model=List[Fase])
def get_fases(
    solo_activos: bool = Query(True, description="True = solo versiones vigentes (para dropdowns)"),
    _admin: dict = Depends(get_current_admin),
):
    return efeti_service.listar_fases(solo_activos)


@router.get("/fases/{fase_id}", response_model=Fase)
def get_fase(fase_id: str, _admin: dict = Depends(get_current_admin)):
    return efeti_service.obtener_fase(fase_id)


@router.post("/fases", response_model=Fase, status_code=status.HTTP_201_CREATED)
def post_fase(payload: FaseCreate, _admin: dict = Depends(get_current_admin)):
    return efeti_service.crear_fase(payload)


@router.put("/fases/{fase_id}", response_model=Fase)
def put_fase(fase_id: str, payload: FaseUpdate, _admin: dict = Depends(get_current_admin)):
    """
    SCD2: cierra la versión actual de la fase (solo modifica activo y fecha_fin,
    preservando el campo `ultimo` histórico) e inserta nueva versión con los datos del payload.
    También crea el registro correspondiente en fase_evento.
    """
    return efeti_service.actualizar_fase_scd2(fase_id, payload)


# ─────────────────────────────────────────────
# EPISODIOS
# ─────────────────────────────────────────────

@router.get("/episodios", response_model=List[Episodio])
def get_episodios(
    solo_activos: bool = Query(True, description="True = solo versiones vigentes (para dropdowns)"),
    _admin: dict = Depends(get_current_admin),
):
    return efeti_service.listar_episodios(solo_activos)


@router.get("/episodios/{episodio_id}", response_model=Episodio)
def get_episodio(episodio_id: str, _admin: dict = Depends(get_current_admin)):
    return efeti_service.obtener_episodio(episodio_id)


@router.post("/episodios", response_model=Episodio, status_code=status.HTTP_201_CREATED)
def post_episodio(payload: EpisodioCreate, _admin: dict = Depends(get_current_admin)):
    return efeti_service.crear_episodio(payload)


@router.put("/episodios/{episodio_id}", response_model=Episodio)
def put_episodio(episodio_id: str, payload: EpisodioUpdate, _admin: dict = Depends(get_current_admin)):
    """
    SCD2: cierra versión actual e inserta nueva.
    Los id_evento_inicio/fin capturan el UUID del evento vigente al momento de la edición.
    """
    return efeti_service.actualizar_episodio_scd2(episodio_id, payload)


# ─────────────────────────────────────────────
# VARIABLES
# ─────────────────────────────────────────────

@router.get("/variables", response_model=List[VariableResumen])
def get_variables(
    tipo: str = Query("all", description="Filtro: 'basica' | 'cambiante' | 'fecha' | 'all'"),
    _admin: dict = Depends(get_current_admin),
):
    return efeti_service.listar_variables(tipo)


@router.get("/variables/{variable_id}/historias", response_model=List[HistoriaVariableCambiante])
def get_historias_variable(variable_id: str, _admin: dict = Depends(get_current_admin)):
    """Devuelve todas las versiones históricas de una variable cambiante."""
    return efeti_service.listar_historias_variable(variable_id)


@router.post("/variables/{variable_id}/convertir-cambiante", response_model=HistoriaVariableCambiante, status_code=status.HTTP_201_CREATED)
def post_convertir_cambiante(
    variable_id: str,
    payload: ConvertirCambianteRequest,
    _admin: dict = Depends(get_current_admin),
):
    """
    Convierte una variable básica en cambiante (SCD2).
    Marca basica=False en la tabla variable e inserta el primer registro en variable_cambiante.
    """
    return efeti_service.convertir_variable_a_cambiante(variable_id, payload)


@router.post("/variables/{variable_id}/agregar-historia", response_model=HistoriaVariableCambiante, status_code=status.HTTP_201_CREATED)
def post_agregar_historia(
    variable_id: str,
    payload: AgregarHistoriaRequest,
    _admin: dict = Depends(get_current_admin),
):
    """
    Agrega una nueva versión histórica a una variable cambiante (SCD2).
    Desactiva todas las historias previas e inserta la nueva con activa=True.
    """
    return efeti_service.agregar_historia_variable(variable_id, payload)
