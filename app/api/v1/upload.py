from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks, Depends
import pandas as pd
import io
import uuid

from app.services.dynamic_validator import run_validation_engine
from app.services.etl_bronze_silver import ejecutar_etl_desde_bronze
from app.core.auth import get_current_user
from app.core.database import supabase_client

# Hoja estándar de la herramienta Canguro que contiene todas las columnas
_HOJA_DATOS = "DB_TOTAL"

router = APIRouter(prefix="/upload", tags=["file upload"])

@router.post("/ingestion/upload")
async def upload_file(
    file: UploadFile = File(...),
    current_user: dict = Depends(get_current_user)
):
    if not file.filename.lower().endswith(('.csv', '.xlsx', '.xls', '.xlsm')):
        raise HTTPException(status_code=400, detail="Only CSV or Excel files (.xlsx, .xlsm) are allowed.")
    
    upload_id = str(uuid.uuid4())
    contents = await file.read()
    
    # Upload to Supabase Storage Bucket 'temp_uploads'
    storage_path = f"{upload_id}_{file.filename}"
    res_storage = supabase_client.storage.from_("temp_uploads").upload(storage_path, contents)
    
    # Crear registro temporal de la sesión de subida
    session_data = {
        "id": upload_id,
        "filename": file.filename,
        "status": "pending"
    }
    
    res_db = supabase_client.table("upload_sessions").insert(session_data).execute()
    
    return {
        "message": "Archivo subido exitosamente a entorno temporal (Storage).", 
        "upload_id": upload_id
    }

def process_validation_background(upload_id: str, filename: str):
    try:
        # Actualizar a 'validating'
        supabase_client.table("upload_sessions").update({"status": "validating"}).eq("id", upload_id).execute()
        
        # Descargar el archivo desde storage
        storage_path = f"{upload_id}_{filename}"
        downloaded = supabase_client.storage.from_("temp_uploads").download(storage_path)
        
        # Leemos con Pandas
        # IMPORTANTE: Si es .xlsm o .xlsx buscamos la hoja DB_TOTAL que es la estándar del proyecto
        if filename.lower().endswith('.csv'):
            df = pd.read_csv(io.BytesIO(downloaded))
        else:
            # Intentamos leer la hoja DB_TOTAL, si no existe leemos la primera
            try:
                df = pd.read_excel(io.BytesIO(downloaded), sheet_name='DB_TOTAL')
            except:
                df = pd.read_excel(io.BytesIO(downloaded))
            
        # Paso Sanity Check + Pydantic dynamically
        valid_records, errors = run_validation_engine(df)
        
        # Guardar en Base de Datos (Reportes de validación)
        report_data = {
            "upload_id": upload_id,
            "errors": errors,
            "total_errors": len(errors)
        }
        supabase_client.table("validation_reports").insert(report_data).execute()
        
        # Actualizar sesión dependiendo de errores
        new_status = "invalid" if len(errors) > 0 else "valid"
        supabase_client.table("upload_sessions").update({"status": new_status}).eq("id", upload_id).execute()
        
    except Exception as e:
        print(f"Error en procesado: {e}")
        supabase_client.table("upload_sessions").update({
            "status": "invalid" 
        }).eq("id", upload_id).execute()

@router.post("/validation/run")
async def run_validation(
    upload_id: str,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(get_current_user)
):
    # Check if session exists
    session_res = supabase_client.table("upload_sessions").select("*").eq("id", upload_id).execute()
    if not session_res.data:
        raise HTTPException(status_code=404, detail="Upload ID no encontrado.")
        
    session = session_res.data[0]
    if session["status"] != "pending":
        raise HTTPException(status_code=400, detail="El archivo ya fue validado o está en proceso.")
        
    filename = session["filename"]
    
    # Start background task
    background_tasks.add_task(process_validation_background, upload_id, filename)
    return {"message": "Motor de validación iniciado en 2do plano.", "upload_id": upload_id}

@router.get("/validation/reports/{upload_id}")
async def get_validation_report(
    upload_id: str,
    current_user: dict = Depends(get_current_user)
):
    session_res = supabase_client.table("upload_sessions").select("status").eq("id", upload_id).execute()
    if not session_res.data:
        raise HTTPException(status_code=404, detail="Upload ID no encontrado.")
        
    status = session_res.data[0]["status"]
    
    if status == "validating":
        return {"status": "validating", "message": "Validación en proceso. Por favor espere."}
        
    reports_res = supabase_client.table("validation_reports").select("*").eq("upload_id", upload_id).execute()
    if not reports_res.data:
        return {"status": status, "message": "No hay reportes disponibles. ¿Ya corriste el motor de validación?"}
        
    report = reports_res.data[0]
    return {
        "status": status,
        "total_errors": report.get("total_errors"),
        "errors": report.get("errors")
    }

def _etl_background(upload_id: str, nombre_fundacion: str) -> None:
    """BackgroundTask: ejecuta el ETL Bronze→Silver después del promote."""
    try:
        ejecutar_etl_desde_bronze(upload_id=upload_id, nombre_fundacion=nombre_fundacion)
    except Exception as e:
        print(f"[ETL background] Error en upload_id={upload_id}: {e}")
        supabase_client.table("upload_sessions").update(
            {"status": "etl_error"}
        ).eq("id", upload_id).execute()


@router.post("/pipeline/promote")
async def promote_to_bronze(
    upload_id: str,
    background_tasks: BackgroundTasks,
    nombre_fundacion: str = "Fundación Canguro",
    current_user: dict = Depends(get_current_user),
):
    session_res = supabase_client.table("upload_sessions").select("*").eq("id", upload_id).execute()
    if not session_res.data:
        raise HTTPException(status_code=404, detail="Upload ID no encontrado.")

    session = session_res.data[0]

    if session["status"] != "valid":
        raise HTTPException(
            status_code=400,
            detail=f"No se puede promover un archivo con estado '{session['status']}'. Debe estar 'valid'."
        )

    # Descargar archivo desde Storage
    storage_path = f"{upload_id}_{session['filename']}"
    downloaded = supabase_client.storage.from_("temp_uploads").download(storage_path)

    # FIX: leer la hoja DB_TOTAL (la estándar de la herramienta Canguro).
    # Si no existe esa hoja (CSV u otro formato), leer la primera.
    if session["filename"].lower().endswith(".csv"):
        df = pd.read_csv(io.BytesIO(downloaded))
    else:
        try:
            df = pd.read_excel(io.BytesIO(downloaded), sheet_name=_HOJA_DATOS)
        except Exception:
            df = pd.read_excel(io.BytesIO(downloaded))

    # Convertir columnas datetime a string ISO para que sean JSON-serializables
    for col in df.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns:
        df[col] = df[col].dt.strftime("%Y-%m-%d")

    df = df.where(pd.notnull(df), None)
    raw_payload = df.to_dict(orient="records")

    bronze_data = {
        "upload_id": upload_id,
        "filename": session["filename"],
        "raw_payload": raw_payload,
    }

    # 1. Insertar en Capa Bronze
    supabase_client.table("bronze_raw_clinical_data").insert(bronze_data).execute()

    # 2. Actualizar estado y limpiar Storage temporal
    supabase_client.table("upload_sessions").update({"status": "promoted"}).eq("id", upload_id).execute()
    supabase_client.storage.from_("temp_uploads").remove([storage_path])

    # 3. Lanzar ETL Bronze→Silver en segundo plano
    background_tasks.add_task(_etl_background, upload_id, nombre_fundacion)

    return {
        "message": "Datos promovidos a Capa Bronze. ETL Bronze→Silver iniciado en segundo plano.",
        "upload_id": upload_id,
        "etl_status": "running"
    }

