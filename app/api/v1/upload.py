from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks, Depends
import pandas as pd
import io
import uuid
from typing import Dict, Any

from app.services.dynamic_validator import generate_dynamic_model, validate_dataframe_chunk
from app.core.auth import get_current_user

router = APIRouter(prefix="/upload", tags=["file upload"])

# Dictionary to hold simple in-memory job status for PMV
job_statuses: Dict[str, Any] = {}

def process_file_background(job_id: str, file_contents: bytes, filename: str):
    try:
        job_statuses[job_id]["status"] = "processing"
        
        # Load rules and generate dynamic Pydantic model
        DynamicModel = generate_dynamic_model()
        
        # Read file with Pandas depending on extension
        if filename.endswith('.csv'):
            # Chunking is natural in CSV
            chunks = pd.read_csv(io.BytesIO(file_contents), chunksize=1000)
            all_errors = []
            total_processed = 0
            
            for chunk in chunks:
                valid_records, errors = validate_dataframe_chunk(chunk, DynamicModel)
                
                # Offset row index by total processed so far
                for err in errors:
                    err["row"] += total_processed
                
                all_errors.extend(errors)
                total_processed += len(chunk)
                
        elif filename.endswith('.xlsx') or filename.endswith('.xls'):
            # Pandas does not support chunksize for Excel directly easily, 
            # so we read all and manually chunk
            df = pd.read_excel(io.BytesIO(file_contents))
            chunk_size = 1000
            all_errors = []
            total_processed = 0
            
            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i:i+chunk_size]
                valid_records, errors = validate_dataframe_chunk(chunk, DynamicModel)
                
                # Offset row index
                for err in errors:
                    err["row"] += total_processed
                    
                all_errors.extend(errors)
                total_processed += len(chunk)
                
        else:
            job_statuses[job_id]["status"] = "failed"
            job_statuses[job_id]["message"] = "Unsupported file format."
            return

        job_statuses[job_id]["status"] = "completed"
        job_statuses[job_id]["processed_rows"] = total_processed
        job_statuses[job_id]["errors"] = all_errors
        job_statuses[job_id]["total_errors"] = len(all_errors)
        
    except Exception as e:
        job_statuses[job_id]["status"] = "failed"
        job_statuses[job_id]["error"] = str(e)


@router.post("/")
async def upload_file(
    background_tasks: BackgroundTasks, 
    file: UploadFile = File(...),
    current_user: dict = Depends(get_current_user)
):
    if not file.filename.endswith(('.csv', '.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="Only CSV or Excel files are allowed.")
    
    # Read file into memory (for PMV. In real prod with massive files, we'd save it to a temp dir directly)
    contents = await file.read()
    
    job_id = str(uuid.uuid4())
    job_statuses[job_id] = {
        "status": "pending",
        "filename": file.filename
    }
    
    background_tasks.add_task(process_file_background, job_id, contents, file.filename)
    
    return {"message": "File upload accepted. Processing in background.", "job_id": job_id}

@router.get("/{job_id}/status")
async def get_job_status(job_id: str):
    if job_id not in job_statuses:
        raise HTTPException(status_code=404, detail="Job ID not found")
    
    return job_statuses[job_id]
