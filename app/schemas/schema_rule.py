from pydantic import BaseModel
from typing import Optional

class SchemaRuleBase(BaseModel):
    column_name: str
    is_required: bool = True
    data_type: str = "str"
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    missing_value_code: Optional[str] = None

class SchemaRuleCreate(SchemaRuleBase):
    pass

class SchemaRuleUpdate(SchemaRuleBase):
    pass

class SchemaRule(SchemaRuleBase):
    id: int

    class Config:
        from_attributes = True
