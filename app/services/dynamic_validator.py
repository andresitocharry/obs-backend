from pydantic import create_model, Field, BaseModel
from typing import Any, Dict, Type, Optional
from app.services.schema_service import get_all_rules

def _map_type(type_str: str) -> Type:
    type_mapping = {
        "str": str,
        "string": str,
        "int": int,
        "integer": int,
        "float": float,
        "bool": bool,
        "boolean": bool
    }
    return type_mapping.get(type_str.lower(), str)

def generate_dynamic_model() -> Type[BaseModel]:
    rules = get_all_rules()
    
    fields: Dict[str, Any] = {}
    for rule in rules:
        col_name = rule.get("column_name")
        data_type = _map_type(rule.get("data_type", "str"))
        is_required = rule.get("is_required", True)
        min_val = rule.get("min_value")
        max_val = rule.get("max_value")
        
        # Build field constraints
        field_kwargs = {}
        if min_val is not None:
            field_kwargs["ge"] = min_val
        if max_val is not None:
            field_kwargs["le"] = max_val
            
        # Determine default value
        default = ... if is_required else None
        
        # Determine the type annotation considering the requirement
        if not is_required:
            data_type = Optional[data_type]
            
        fields[col_name] = (data_type, Field(default, **field_kwargs))
        
    DynamicSchemaModel = create_model('DynamicSchemaModel', **fields)
    return DynamicSchemaModel

def validate_dataframe_chunk(df_chunk, DynamicModel: Type[BaseModel]):
    records = df_chunk.to_dict(orient='records')
    valid_records = []
    errors = []
    
    for idx, record in enumerate(records):
        try:
            validated = DynamicModel(**record)
            valid_records.append(validated.model_dump())
        except Exception as e:
            errors.append({
                "row": idx,
                "error": str(e)
            })
            
    return valid_records, errors
