from fastapi import APIRouter, Depends, HTTPException, status
from typing import List

from app.schemas.schema_rule import SchemaRule, SchemaRuleCreate, SchemaRuleUpdate
from app.services.schema_service import get_all_rules, create_rule, update_rule, delete_rule
from app.services.indicator_service import get_indicator_dependencies_for_rule
from app.core.auth import get_current_user

def get_current_admin(user_data: dict = Depends(get_current_user)):
    role = user_data.get("role")
    if role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Se requieren permisos de administrador"
        )
    return user_data

router = APIRouter(prefix="/admin/schema", tags=["schema management"])

@router.get("/", response_model=List[SchemaRule])
async def read_rules(current_admin: dict = Depends(get_current_admin)):
    return get_all_rules()

@router.post("/", response_model=SchemaRule)
async def create_new_rule(rule: SchemaRuleCreate, current_admin: dict = Depends(get_current_admin)):
    return create_rule(rule)

@router.put("/{rule_id}", response_model=SchemaRule)
async def update_existing_rule(rule_id: str, rule: SchemaRuleUpdate, current_admin: dict = Depends(get_current_admin)):
    return update_rule(rule_id, rule)

@router.delete("/{rule_id}")
async def remove_rule(rule_id: str, current_admin: dict = Depends(get_current_admin)):
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
