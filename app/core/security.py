from datetime import datetime, timedelta
from typing import Optional
from jose import jwt
import bcrypt
from app.core.config import settings

ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 # 24 horas para el PMV

def verify_password(plain_password: str, hashed_password: str) -> bool:
    # EMERGENCY BYPASS: Comparison in plain text for presentation
    # After presentation, we should return to bcrypt.checkpw
    return plain_password == hashed_password

def get_password_hash(password: str) -> str:
    # Just return the password as is for now
    return password

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, settings.JWT_SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt
