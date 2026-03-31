from typing import Optional
from pydantic import BaseModel

class Token(BaseModel):
    access_token: str
    token_type: str
    role: str

class TokenData(BaseModel):
    username: Optional[str] = None
    role: Optional[str] = None
    
class AdminUserIn(BaseModel):
    username: str
    password: str

class UserCreate(BaseModel):
    username: str
    password: str
    role: str = "foundation"

class UserOut(BaseModel):
    id: Optional[str] = None
    username: str
    role: str
    created_at: Optional[str] = None
