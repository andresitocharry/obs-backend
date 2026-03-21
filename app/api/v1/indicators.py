from fastapi import APIRouter, Depends, HTTPException
from typing import List
from app.schemas.indicator import IndicatorWithDependencies
from app.services.indicator_service import get_all_indicators
from app.api.v1.schema import get_current_admin

router = APIRouter(prefix="/admin/indicators", tags=["indicators management"])

@router.get("/", response_model=List[IndicatorWithDependencies])
async def read_indicators(current_admin: dict = Depends(get_current_admin)):
    return get_all_indicators()
