from app.core.database import supabase_client

def get_all_indicators():
    response = supabase_client.table("indicators").select("*, indicator_dependencies(schema_rules(column_name))").execute()
    # Flatten the dependencies for the UI
    data = response.data
    for item in data:
        deps = item.pop("indicator_dependencies", [])
        item["dependencies"] = [d["schema_rules"]["column_name"] for d in deps if d.get("schema_rules")]
    return data

def get_indicator_dependencies_for_rule(rule_id: int):
    response = supabase_client.table("indicator_dependencies")\
        .select("indicators(name)")\
        .eq("rule_id", rule_id).execute()
    return [d["indicators"]["name"] for d in response.data if d.get("indicators")]
