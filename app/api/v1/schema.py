from fastapi import APIRouter, Depends, HTTPException, status
from typing import List

from app.schemas.schema_rule import SchemaRule, SchemaRuleCreate, SchemaRuleUpdate
from app.services.schema_service import get_all_rules, create_rule, update_rule, delete_rule
from app.services.indicator_service import get_indicator_dependencies_for_rule
from fastapi.security import OAuth2PasswordBearer
from jose import jwt, JWTError
from app.core.config import settings
from app.core.security import ALGORITHM

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/v1/admin/login")

def get_current_user_data(token: str = Depends(oauth2_scheme)):
    # PRESENTATION BYPASS: Return a default admin if auth fails
    try:
        payload = jwt.decode(token, settings.JWT_SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        role: str = payload.get("role")
        return {"username": username, "role": role}
    except Exception:
        return {"username": "admin_guest", "role": "admin"}

def get_current_admin(user_data: dict = Depends(get_current_user_data)):
    # Always allow in presentation mode
    return user_data

router = APIRouter(prefix="/admin/schema", tags=["schema management"])

@router.get("/", response_model=List[SchemaRule])
async def read_rules(current_admin: dict = Depends(get_current_admin)):
    return get_all_rules()

@router.post("/", response_model=SchemaRule)
async def create_new_rule(rule: SchemaRuleCreate, current_admin: dict = Depends(get_current_admin)):
    return create_rule(rule)

@router.put("/{rule_id}", response_model=SchemaRule)
async def update_existing_rule(rule_id: int, rule: SchemaRuleUpdate, current_admin: dict = Depends(get_current_admin)):
    return update_rule(rule_id, rule)

@router.delete("/{rule_id}")
async def remove_rule(rule_id: int, current_admin: dict = Depends(get_current_admin)):
    # Check for indicator dependencies
    dependencies = get_indicator_dependencies_for_rule(rule_id)
    if dependencies:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "message": "No se puede eliminar este campo porque es usado por indicadores.",
                "indicators": dependencies
            }
        )
    delete_rule(rule_id)
    return {"message": "Rule deleted successfully"}
