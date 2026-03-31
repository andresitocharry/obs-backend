from fastapi import APIRouter, Depends, HTTPException
from typing import List
from app.schemas.indicator import IndicatorWithDependencies
from app.services.indicator_service import get_all_indicators
from app.core.auth import get_current_user

router = APIRouter(prefix="/admin/indicators", tags=["indicators management"])

@router.get("/", response_model=List[IndicatorWithDependencies])
async def read_indicators(current_user: dict = Depends(get_current_user)):
    return get_all_indicators()
