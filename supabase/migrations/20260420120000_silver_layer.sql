-- ============================================================
-- Migración: Capa Silver — tablas clínicas transaccionales
-- Fecha: 2026-04-20
-- Descripción: Crea la dimensión fundacion, la dimensión paciente
--   y la tabla de hechos hecho_medicion_paciente.
--   Resuelve la Brecha 2 del Reporte de Estado Actual (gap del
--   fact table de observaciones reales de pacientes).
-- ============================================================


-- ─────────────────────────────────────────────────────────────
-- 1. Dimensión FUNDACION
--    Representa la institución/fundación que reporta los datos.
--    Permite auditoría multi-institución en el futuro.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.fundacion (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    nombre      VARCHAR(255) NOT NULL,
    codigo_pais VARCHAR(10),
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE  public.fundacion              IS 'Instituciones/fundaciones que reportan datos al Observatorio Canguro';
COMMENT ON COLUMN public.fundacion.codigo_pais  IS 'Código ISO 3166-1 numérico del país (campo COD_PAIS del Excel)';


-- ─────────────────────────────────────────────────────────────
-- 2. Dimensión PACIENTE
--    Representa a cada bebé del programa Canguro.
--    id_canguro es la llave de negocio que viene en los Excel.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.paciente (
    id                   UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    id_canguro           VARCHAR(100) NOT NULL UNIQUE,  -- ID_CANGURO del Excel: llave de negocio
    id_nacional          VARCHAR(100),                   -- ID_NACIONAL del Excel
    cod_pais             VARCHAR(10),
    id_fundacion         UUID         REFERENCES public.fundacion(id) ON DELETE SET NULL,
    created_at           TIMESTAMPTZ  DEFAULT NOW()
);

COMMENT ON TABLE  public.paciente              IS 'Bebés del programa Canguro. Un paciente puede tener mediciones de varias fundaciones.';
COMMENT ON COLUMN public.paciente.id_canguro   IS 'Identificador del programa Canguro (ID_CANGURO en Excel). Llave de negocio única.';
COMMENT ON COLUMN public.paciente.id_nacional  IS 'Identificador nacional del paciente (ID_NACIONAL en Excel). Puede ser null si no aplica.';


-- ─────────────────────────────────────────────────────────────
-- 3. Tabla de Hechos: HECHO_MEDICION_PACIENTE
--    Corazón de la Capa Silver. Formato largo (una fila por
--    paciente × variable × fecha). Vincula las observaciones
--    reales con el modelo de metadatos EFETI.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.hecho_medicion_paciente (
    id           UUID         PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Claves foráneas dimensionales
    id_paciente  UUID         NOT NULL REFERENCES public.paciente(id)          ON DELETE CASCADE,
    id_variable  UUID         NOT NULL REFERENCES public.variable(id)          ON DELETE RESTRICT,
    id_fecha     VARCHAR(8)   REFERENCES public.fecha(id)                      ON DELETE SET NULL,   -- DDMMYYYY, nullable
    id_fase      UUID         REFERENCES public.fase(id)                       ON DELETE SET NULL,   -- fase vigente al registrar
    id_episodio  UUID         REFERENCES public.episodio(id)                   ON DELETE SET NULL,   -- episodio activo si aplica

    -- El valor real de la medición (siempre TEXT para soportar numérico, categórico y cadena)
    valor        TEXT,

    -- Trazabilidad del origen del dato
    id_upload    UUID         REFERENCES public.upload_sessions(id)            ON DELETE SET NULL,   -- archivo Bronze de origen
    id_fundacion UUID         REFERENCES public.fundacion(id)                  ON DELETE SET NULL,   -- institución reportante

    created_at   TIMESTAMPTZ  DEFAULT NOW(),

    -- Constraint de unicidad para idempotencia del ETL.
    -- NULLS NOT DISTINCT: dos filas con id_fecha NULL se consideran duplicadas si
    -- tienen el mismo id_paciente + id_variable (requiere PostgreSQL 15+, Supabase lo soporta).
    CONSTRAINT uq_medicion_paciente_variable_fecha
        UNIQUE NULLS NOT DISTINCT (id_paciente, id_variable, id_fecha)
);

COMMENT ON TABLE  public.hecho_medicion_paciente            IS 'Tabla de hechos: observaciones clínicas reales de pacientes (Capa Silver). Formato largo: 1 fila por paciente × variable × fecha.';
COMMENT ON COLUMN public.hecho_medicion_paciente.valor      IS 'Valor de la medición como texto. Numérico, código categórico o cadena según tipo_dato de la variable EFETI.';
COMMENT ON COLUMN public.hecho_medicion_paciente.id_fecha   IS 'FK a fecha.id (formato DDMMYYYY). NULL si la medición no tiene fecha específica.';
COMMENT ON COLUMN public.hecho_medicion_paciente.id_fase    IS 'Fase del protocolo Canguro vigente cuando se tomó la medición. NULL si no se puede determinar.';
COMMENT ON COLUMN public.hecho_medicion_paciente.id_upload  IS 'Referencia al archivo Bronze del que proviene este dato (trazabilidad completa fuente → Silver).';

-- Índices para consultas analíticas frecuentes
CREATE INDEX IF NOT EXISTS idx_medicion_paciente    ON public.hecho_medicion_paciente (id_paciente);
CREATE INDEX IF NOT EXISTS idx_medicion_variable    ON public.hecho_medicion_paciente (id_variable);
CREATE INDEX IF NOT EXISTS idx_medicion_fecha       ON public.hecho_medicion_paciente (id_fecha);
CREATE INDEX IF NOT EXISTS idx_medicion_upload      ON public.hecho_medicion_paciente (id_upload);
