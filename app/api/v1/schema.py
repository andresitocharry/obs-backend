from fastapi import APIRouter, Depends, HTTPException, status
from typing import List

from app.schemas.schema_rule import SchemaRule, SchemaRuleCreate, SchemaRuleUpdate
from app.services.schema_service import get_all_rules, create_rule, update_rule, delete_rule
# Dependencia de seguridad (Dummy for now, here we check the JWT)
def get_current_admin():
    # TODO: Implement token decode and user fetch
    return {"username": "admin"}

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
    delete_rule(rule_id)
    return {"message": "Rule deleted successfully"}
