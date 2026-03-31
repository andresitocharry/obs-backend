from fastapi import HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from app.core.security import verify_password, create_access_token, ACCESS_TOKEN_EXPIRE_MINUTES
from app.core.database import supabase_client
from datetime import timedelta

def authenticate_user(form_data: OAuth2PasswordRequestForm):
    # Buscar usuario por username
    response = supabase_client.table("users").select("*").eq("username", form_data.username).execute()
    users = response.data
    print(f"DEBUG: Buscando usuario '{form_data.username}'. Encontrados: {len(users)}")
    
    if not users:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    user = users[0]
    
    # Validar pass
    is_valid = verify_password(form_data.password, user['hashed_password'])
    print(f"DEBUG: Autenticando usuario '{form_data.username}'. ¿Password coincide?: {is_valid}")
    
    if not is_valid:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
        
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user["username"], "role": user["role"]}, expires_delta=access_token_expires
    )
    
    return {"access_token": access_token, "token_type": "bearer", "role": user["role"]}

def create_user(user_data):
    from app.core.security import get_password_hash
    # Verificar si el usuario ya existe
    response = supabase_client.table("users").select("username").eq("username", user_data.username).execute()
    if response.data:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Username already registered"
        )
    
    # Crear usuario
    hashed_pass = get_password_hash(user_data.password)
    new_user = {
        "username": user_data.username,
        "hashed_password": hashed_pass,
        "role": user_data.role
    }
    
    response = supabase_client.table("users").insert(new_user).execute()
    if not response.data:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error creating user"
        )
        
    return response.data[0]

def get_all_users():
    response = supabase_client.table("users").select("id, username, role, created_at").execute()
    return response.data
