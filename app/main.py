from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.v1 import admin, schema, upload, indicators, efeti, etl

app = FastAPI(
    title="Data Quality Gate API",
    description="API for validating clinical data for Fundación Canguro",
    version="1.0.0"
)

import os

# Configurar CORS para producción
CORS_ORIGINS = os.getenv("CORS_ORIGINS", "*").split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:3001",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:3001",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(admin.router, prefix="/api/v1")
app.include_router(schema.router, prefix="/api/v1")
app.include_router(upload.router, prefix="/api/v1")
app.include_router(indicators.router, prefix="/api/v1")
app.include_router(efeti.router, prefix="/api/v1")
app.include_router(etl.router, prefix="/api/v1")

@app.get("/")
def read_root():
    return {"message": "Data Quality Gate API is running"}
