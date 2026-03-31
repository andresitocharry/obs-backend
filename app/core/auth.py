from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt, JWTError
from app.core.config import settings

security = HTTPBearer()

def get_current_user(token: HTTPAuthorizationCredentials = Depends(security)):
    """
    Verifica el token JWT enviado por Supabase.
    En una integración profesional, validaríamos el token contra las llaves públicas de Supabase
    o simplemente usaríamos el cliente de Supabase para verificar la sesión.
    """
    try:
        # En Supabase, el JWT está firmado con el JWT_SECRET_KEY del proyecto
        payload = jwt.decode(
            token.credentials, 
            settings.JWT_SECRET_KEY, 
            algorithms=["HS256"]
        )
        return payload
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
