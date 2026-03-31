from fastapi import APIRouter, Depends
from fastapi.security import OAuth2PasswordRequestForm
from app.schemas.auth import Token, UserCreate, UserOut
from app.services.auth_service import authenticate_user, create_user, get_all_users
from app.core.auth import get_current_user
from typing import List
from fastapi import HTTPException

router = APIRouter(prefix="/admin", tags=["admin"])

@router.post("/login", response_model=Token)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    return authenticate_user(form_data)

@router.post("/register", response_model=UserOut)
async def register_new_user(user_in: UserCreate, current_user: dict = Depends(get_current_user)):
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Not authorized to create users")
    return create_user(user_in)

@router.get("/users", response_model=List[UserOut])
async def list_users(current_user: dict = Depends(get_current_user)):
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Not authorized to view users")
    return get_all_users()
