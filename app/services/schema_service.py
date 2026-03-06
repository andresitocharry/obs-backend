from app.core.database import supabase_client
from app.schemas.schema_rule import SchemaRuleCreate, SchemaRuleUpdate

def get_all_rules():
    response = supabase_client.table("schema_rules").select("*").execute()
    return response.data

def create_rule(rule: SchemaRuleCreate):
    response = supabase_client.table("schema_rules").insert(rule.model_dump()).execute()
    return response.data[0] if response.data else None

def update_rule(rule_id: int, rule: SchemaRuleUpdate):
    response = supabase_client.table("schema_rules").update(rule.model_dump()).eq("id", rule_id).execute()
    return response.data[0] if response.data else None

def delete_rule(rule_id: int):
    response = supabase_client.table("schema_rules").delete().eq("id", rule_id).execute()
    return response.data
