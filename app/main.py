from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.v1 import admin, schema, upload

app = FastAPI(
    title="Data Quality Gate API",
    description="API for validating clinical data for Fundación Canguro",
    version="1.0.0"
)

# Configurar CORS (ajustar en producción)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Permitir todos los orígenes por ahora (Frontend en local o Docker)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(admin.router, prefix="/api/v1")
app.include_router(schema.router, prefix="/api/v1")
app.include_router(upload.router, prefix="/api/v1")

@app.get("/")
def read_root():
    return {"message": "Data Quality Gate API is running"}
