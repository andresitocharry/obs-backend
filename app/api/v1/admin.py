from fastapi import APIRouter, Depends
from fastapi.security import OAuth2PasswordRequestForm
from app.schemas.auth import Token
from app.services.auth_service import authenticate_admin

router = APIRouter(prefix="/admin", tags=["admin"])

@router.post("/login", response_model=Token)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    return authenticate_admin(form_data)
