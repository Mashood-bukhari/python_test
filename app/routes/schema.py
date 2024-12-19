from pydantic import BaseModel
from datetime import datetime


class User(BaseModel):
    username: str
    password: str
    
class Expense(BaseModel):
    amount: float
    category: str
    description: str 
    date: datetime
    

    