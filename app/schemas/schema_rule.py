from pydantic import BaseModel, UUID4
from typing import Optional, Union

class SchemaRuleBase(BaseModel):
    column_name: str
    column_display_name: Optional[str] = None
    is_required: bool = True
    data_type: str = "str"
    basica: bool = False
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    missing_value_code: Optional[str] = None
    fase_nombre: Optional[str] = None
    evento_nombre: Optional[str] = None

class SchemaRuleCreate(SchemaRuleBase):
    pass

class SchemaRuleUpdate(SchemaRuleBase):
    pass

class SchemaRule(SchemaRuleBase):
    id: str # Soporta IDs incrementales (int) o UUIDs (str)

    class Config:
        from_attributes = True
