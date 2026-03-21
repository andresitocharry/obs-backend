from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

class IndicatorBase(BaseModel):
    name: str
    description: Optional[str] = None
    calculation_formula: str

class IndicatorCreate(IndicatorBase):
    pass

class Indicator(IndicatorBase):
    id: int
    created_at: datetime

    class Config:
        from_attributes = True

class IndicatorWithDependencies(Indicator):
    dependencies: List[str] # List of column_names
