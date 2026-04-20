from app.core.database import supabase_client
from app.schemas.schema_rule import SchemaRuleCreate, SchemaRuleUpdate

from app.core.database import supabase_client

def get_validation_rules():
    """
    Lee dinámicamente las reglas de validación y la gobernanza desde el modelo EFETI.
    Relaciona: Variable -> Hechos -> Fase & Evento.
    """
    # Usamos la sintaxis de PostgREST para joins profundos
    response = supabase_client.table("variable") \
        .select("""
            id, 
            nombre_bd, 
            nombre_analisis, 
            tipo_dato, 
            basica,
            hecho_registrar_variable(
                valor_min, 
                valor_max, 
                valor_no_conocido,
                fase(nombre_analisis)
            )
        """) \
        .execute()
        
    raw_data = response.data
    rules = []
    
    for var in raw_data:
        hechos = var.get("hecho_registrar_variable", [])
        hecho = hechos[0] if hechos and isinstance(hechos, list) else {}
        
        # Extraer nombres de Fase (Gobernanza)
        fase_info = hecho.get("fase", {})
        
        rule = {
            "id": var.get("id"),
            "column_name": var.get("nombre_bd"),
            "column_display_name": var.get("nombre_analisis"),
            "data_type": str(var.get("tipo_dato", "Numérico")).lower(),
            "basica": var.get("basica", False),
            "min_value": hecho.get("valor_min"),
            "max_value": hecho.get("valor_max"),
            "missing_value": hecho.get("valor_no_conocido"),
            # Gobernanza EFETI
            "fase_nombre": fase_info.get("nombre_analisis", "No asignada"),
            "evento_nombre": "Sin evento" # Simplificado para corregir el crash
        }
        rules.append(rule)
        
    return rules

# Alias y Stubs para compatibilidad con app/api/v1/schema.py y evitar el crash de booteo
def get_all_rules():
    return get_validation_rules()

def create_rule(rule: SchemaRuleCreate):
    # Por ahora el modelo EFETI es solo lectura desde el UI (se carga vía uploader)
    return {"message": "Not implemented for EFETI model"}

def update_rule(rule_id: int, rule: SchemaRuleUpdate):
    return {"message": "Not implemented for EFETI model"}

def delete_rule(rule_id: int):
    return {"message": "Not implemented for EFETI model"}

