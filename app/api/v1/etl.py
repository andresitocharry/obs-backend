from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks

from app.services.etl_bronze_silver import ejecutar_etl_desde_bronze
from app.core.auth import get_current_user
from app.core.database import supabase_client


def get_current_admin(user_data: dict = Depends(get_current_user)):
    if user_data.get("role") != "admin":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Se requieren permisos de administrador")
    return user_data


router = APIRouter(prefix="/etl", tags=["ETL — Bronze → Silver"])


@router.post("/run")
async def run_etl(
    upload_id: str,
    background_tasks: BackgroundTasks,
    nombre_fundacion: str = "Fundación Canguro",
    _admin: dict = Depends(get_current_admin),
):
    """
    Dispara manualmente el ETL Bronze→Silver para un upload_id dado.
    Útil para re-procesar un archivo que ya fue promovido a Bronze
    o para reintentar después de un etl_error.
    El proceso corre en segundo plano — consulta el estado con GET /etl/status/{upload_id}.
    """
    # Verificar que el upload existe y tiene datos en Bronze
    bronze_res = supabase_client.table("bronze_raw_clinical_data") \
        .select("upload_id") \
        .eq("upload_id", upload_id) \
        .execute()

    if not bronze_res.data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No hay datos en Bronze para upload_id={upload_id}. Primero haz promote."
        )

    def _run(uid: str, fundacion: str):
        try:
            ejecutar_etl_desde_bronze(upload_id=uid, nombre_fundacion=fundacion)
        except Exception as e:
            print(f"[ETL manual] Error en upload_id={uid}: {e}")
            supabase_client.table("upload_sessions").update(
                {"status": "etl_error"}
            ).eq("id", uid).execute()

    background_tasks.add_task(_run, upload_id, nombre_fundacion)

    return {
        "message": "ETL Bronze→Silver iniciado en segundo plano.",
        "upload_id": upload_id,
        "fundacion": nombre_fundacion,
    }


@router.get("/status/{upload_id}")
async def get_etl_status(upload_id: str, _user: dict = Depends(get_current_user)):
    """
    Consulta el estado del ETL para un upload_id.
    Estados posibles: promoted | silver_processed | etl_error
    """
    session_res = supabase_client.table("upload_sessions") \
        .select("id, filename, status") \
        .eq("id", upload_id) \
        .execute()

    if not session_res.data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Upload ID no encontrado.")

    session = session_res.data[0]

    # Contar mediciones insertadas en Silver para este upload
    count_res = supabase_client.table("hecho_medicion_paciente") \
        .select("id", count="exact") \
        .eq("id_upload", upload_id) \
        .execute()

    mediciones_silver = count_res.count if hasattr(count_res, "count") else 0

    return {
        "upload_id": upload_id,
        "filename": session["filename"],
        "pipeline_status": session["status"],
        "mediciones_en_silver": mediciones_silver,
    }
