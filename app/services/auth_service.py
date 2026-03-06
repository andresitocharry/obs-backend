from fastapi import HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from app.core.security import verify_password, create_access_token, ACCESS_TOKEN_EXPIRE_MINUTES
from app.core.database import supabase_client
from datetime import timedelta

def authenticate_admin(form_data: OAuth2PasswordRequestForm):
    # Buscar usuario por username (u otro identificador)
    response = supabase_client.table("admin_users").select("*").eq("username", form_data.username).execute()
    users = response.data
    
    if not users:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    user = users[0]
    
    # Validar pass
    if not verify_password(form_data.password, user['hashed_password']):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
        
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user["username"], "role": "admin"}, expires_delta=access_token_expires
    )
    
    return {"access_token": access_token, "token_type": "bearer"}
