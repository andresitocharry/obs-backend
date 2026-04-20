from pydantic import create_model, Field, BaseModel, ValidationError
from typing import Any, Dict, Type, Optional, List
import pandas as pd
import math

from app.services.schema_service import get_validation_rules

def _map_python_type(type_str: str) -> Type:
    type_str = type_str.lower() if type_str else ""
    if "num" in type_str or "float" in type_str or "int" in type_str:
        return float # Usamos float para evitar problemas de NaN con Pandas
    if "bool" in type_str:
        return bool
    return str

def generate_dynamic_model() -> Type[BaseModel]:
    rules = get_validation_rules()
    
    fields: Dict[str, Any] = {}
    for rule in rules:
        col_name = rule.get("column_name")
        if not col_name:
            continue
            
        data_type = _map_python_type(rule.get("data_type"))
        min_val = rule.get("min_value")
        max_val = rule.get("max_value")
        
        # Pydantic Fields settings
        field_kwargs = {}
        if min_val is not None and data_type == float:
            field_kwargs["ge"] = float(min_val)
        if max_val is not None and data_type == float:
            field_kwargs["le"] = float(max_val)
            
        # Permitimos nulos para que el validador no reviente por celdas vacias de Excel
        # que pandas lee como NaN (y luego procesamos a None)
        fields[col_name] = (Optional[data_type], Field(None, **field_kwargs))
        
    DynamicSchemaModel = create_model('DynamicSchemaModel', **fields)
    return DynamicSchemaModel

def run_validation_engine(df: pd.DataFrame) -> (List[dict], List[dict]):
    """
    Paso A, B, C y D: 
    Evalúa el DataFrame contra las reglas de EFETI y atrapa los errores sin abortar.
    Implementa un Mapeo Inteligente para normalizar cabeceras de Excel (ej: PESO_NACER -> CP_pesoNacer)
    """
    # 1. Obtener reglas para conocer los nombres reales en DB
    rules = get_validation_rules()
    
    # 2. Mapeo Inteligente de Cabeceras
    # Creamos un mapa de 'Nombre Normalizado' -> 'Nombre Real en DB'
    # Normalización: Quitar prefijo (ej: CP_), quitar guiones bajos y poner en Mayúsculas
    db_mapping = {}
    for r in rules:
        real_name = r.get("column_name")
        if not real_name: continue
        
        # Normalizar nombre de DB (ej: CP_pesoNacer -> PESONACER)
        norm_db = real_name.split("_", 1)[-1].replace("_", "").upper()
        db_mapping[norm_db] = real_name

    # Renombrar columnas del DataFrame basado en el mapeo
    new_columns = {}
    unmapped_columns = []
    for col in df.columns:
        norm_col = str(col).replace("_", "").upper()
        if norm_col in db_mapping:
            new_columns[col] = db_mapping[norm_col]
        else:
            unmapped_columns.append(col)
            
    df = df.rename(columns=new_columns)
    
    # 3. Reemplazar NaN de Pandas por None para que Pydantic lo entienda como nulo (missing object)
    df = df.where(pd.notnull(df), None)
    
    records = df.to_dict(orient='records')
    DynamicModel = generate_dynamic_model()
    
    valid_records = []
    errors = []
    
    # Identificar qué variables tienen un "valor_no_conocido" (Ej: -2, -9) para ignorar rango en esos casos
    missing_value_map = {r.get("column_name"): r.get("missing_value") for r in rules if r.get("missing_value") is not None}
    
    for idx, record in enumerate(records):
        try:
            # Bypass manual para "valor_no_conocido"
            for col, val in record.items():
                if col in missing_value_map and val == missing_value_map[col]:
                    record[col] = None # Lo tratamos como None válido temporalmente
            
            # Ejecutar modelo Pydantic dinámico
            validated = DynamicModel.model_validate(record)
            valid_records.append(validated.model_dump())
            
        except ValidationError as e:
            # Parsear los errores detallados
            for error_detail in e.errors():
                errors.append({
                    "row": idx + 1, # Base 1 para usuarios de Excel
                    "column": str(error_detail["loc"][0]) if error_detail["loc"] else "Desconocida",
                    "error": error_detail["msg"],
                    "type": error_detail["type"],
                    "value_provided": record.get(error_detail["loc"][0]) if error_detail["loc"] else None
                })
        except Exception as e:
            errors.append({
                "row": idx + 1,
                "column": "General",
                "error": str(e)
            })
            
    return valid_records, errors

