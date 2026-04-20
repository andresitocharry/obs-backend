from pydantic import BaseModel
from typing import Optional


# ──────────────────────────────────────────────
# EVENTO
# ──────────────────────────────────────────────

class EventoBase(BaseModel):
    nombre: str
    descripcion: Optional[str] = None
    id_variable_fecha: str  # UUID de la variable tipo DATE asociada

class EventoCreate(EventoBase):
    pass

class EventoUpdate(EventoBase):
    pass

class Evento(EventoBase):
    id: str
    activo: bool
    fecha_inicio: str
    fecha_fin: str

    class Config:
        from_attributes = True


# ──────────────────────────────────────────────
# FASE
# Los eventos de inicio/fin viven en la tabla puente fase_evento,
# pero los recibimos en el payload para crearlos junto con la fase.
# ──────────────────────────────────────────────

class FaseBase(BaseModel):
    nombre_analisis: str
    nombre_bd: str
    descripcion: Optional[str] = None
    num_fase: int
    ultimo: bool = False           # bandera clínica: última fase del protocolo
    id_evento_inicio: str          # UUID del evento activo
    id_evento_fin: str             # UUID del evento activo

class FaseCreate(FaseBase):
    pass

class FaseUpdate(FaseBase):
    pass

class Fase(BaseModel):
    id: str
    nombre_analisis: str
    nombre_bd: str
    descripcion: Optional[str] = None
    num_fase: int
    ultimo: bool
    activo: bool
    fecha_inicio: str
    fecha_fin: str

    class Config:
        from_attributes = True


# ──────────────────────────────────────────────
# EPISODIO
# ──────────────────────────────────────────────

class EpisodioBase(BaseModel):
    nombre_analisis: str
    nombre_bd: str
    descripcion: Optional[str] = None
    id_evento_inicio: str   # UUID del evento (versión vigente al momento del SCD2)
    id_evento_fin: str      # UUID del evento (versión vigente al momento del SCD2)

class EpisodioCreate(EpisodioBase):
    pass

class EpisodioUpdate(EpisodioBase):
    pass

class Episodio(EpisodioBase):
    id: str
    activo: bool
    fecha_inicio: str
    fecha_fin: str

    class Config:
        from_attributes = True


# ──────────────────────────────────────────────
# VARIABLE (listados y conversión)
# ──────────────────────────────────────────────

class VariableResumen(BaseModel):
    id: str
    nombre_analisis: str
    nombre_bd: str
    tipo_dato: Optional[str] = None
    basica: bool

    class Config:
        from_attributes = True


class ConvertirCambianteRequest(BaseModel):
    id_variable_fecha_inicio: str   # UUID de variable tipo DATE (inicio del tramo)
    id_variable_fecha_fin: str      # UUID de variable tipo DATE (fin del tramo)


class AgregarHistoriaRequest(BaseModel):
    id_variable_fecha_inicio: str   # UUID de variable tipo DATE
    id_variable_fecha_fin: str      # UUID de variable tipo DATE


class HistoriaVariableCambiante(BaseModel):
    id_historia: str
    id: str                         # FK a variable
    variable_fecha_inicio: str      # UUID variable fecha inicio
    variable_fecha_fin: str         # UUID variable fecha fin
    activa: bool

    class Config:
        from_attributes = True
