import os
import sys

# Agregar el directorio actual al path para que Python encuentre 'app'
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from app.core.security import get_password_hash
    from app.core.database import supabase_client
except ModuleNotFoundError as e:
    print(f"Error de importación: {e}")
    print("Asegúrate de ejecutar este script desde el entorno virtual o contenedor donde están instaladas las dependencias.")
    sys.exit(1)

def create_admin():
    print("Verificando si el usuario 'admin' existe...")
    # Verificamos si existe admin
    res = supabase_client.table("users").select("username").eq("username", "admin").execute()
    
    if res.data:
        print("⚠️ El usuario 'admin' ya existe en la base de datos.")
        # Si ya existe podríamos actualizarle la contraseña para asegurar que sea adm1n123, pero por ahora solo avisamos
        print("Si no recuerdas la contraseña de este usuario, bórralo desde el panel de Supabase y vuelve a correr este script.")
        return

    print("Creando usuario administrador principal...")
    hashed_pass = get_password_hash("admin123")
    new_user = {
        "username": "admin",
        "hashed_password": hashed_pass,
        "role": "admin"
    }
    
    insert_res = supabase_client.table("users").insert(new_user).execute()
    
    if insert_res.data:
        print("\n✅ ¡Usuario Administrador creado exitosamente!")
        print("👉 Usuario: admin")
        print("👉 Contraseña: admin123")
        print("¡Ya puedes usar estas credenciales para entrar al sistema!")
    else:
        print("❌ Ocurrió un error al crear el usuario. Revisa las políticas o conexión de Supabase.")

if __name__ == "__main__":
    create_admin()
