from fastapi import FastAPI, HTTPException, Depends
from app.routes.schema import User, Expense
from odmantic import AIOEngine, Model

from passlib.context import CryptContext
from datetime import datetime, timedelta
from jose import JWTError, jwt
from typing import Optional
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
import motor.motor_asyncio

SECRET_KEY = "python"
ALGORITHM = 'HS256'
EXPIRE_TIME = 5

app = FastAPI()
engine = AIOEngine()

pwd_context = CryptContext(schemes=['bcrypt'])

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

MONGO_URI = "mongodb://localhost:27017"  # Example URI
DB_NAME = "personal"  # Replace with your database name
client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
db = client[DB_NAME]
user_col = db['users']
Expense_col = db["expense"]


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def verify_token(token: str = Depends(oauth2_scheme)):
    try:
        # Decode the token and extract payload
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        username: str = payload.get("sub")  # Get the username from the payload
        email: str = payload.get("email")    # Get the email from the payload
        if username is None or email is None:
            raise HTTPException(status_code=401, detail="Invalid token")
        return {"username": username, "email": email}
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

@app.post("/register")
async def register(user:User):
    if 'users' not in await db.list_collection_names():
        await db.create_collection("users")
    if 'expense' not in await db.list_collection_names():
       await db.create_collection("expense")
    e_user = await user_col.find_one({'username':user.username})
    
    if e_user:
        raise HTTPException(status_code=400, detail="Already in the DB")
    h_pass = pwd_context.hash(user.password)
    await user_col.insert_one({"username":user.username,"password":h_pass})
    token = create_access_token(data={'sub':user.username})
    return {"message": "User registered","token":token}
        
@app.post("/login")
async def login(user:User): 
    user = await user_col.find_one({"username": user.username})
    
    if not user or not pwd_context.verify(user.password, user['password']):
        raise HTTPException(status_code=400, detail="Not correct password")
    
    token = create_access_token(data={'sub':user.username})
    return {"message": "User logged in","token":token}
        
        
@app.post("/expense")
async def create_expense (request: Expense,current_user: str = Depends(verify_token) ): #current_user: str = Depends(verify_token)
    e = {"user":current_user, **request.dict()}
    result = await Expense_col.insert_one(e)
    return {"id": str(result.inserted_id),"amount": e["amount"],'category':e["category"], 'description':e['description'], "date":e["date"]}



@app.get("/expenses")
async def get_expenses(start_date:datetime=None, end_date: datetime =None,current_user: str = Depends(verify_token) ):
    query = {"user":current_user }
    
    if start_date:
        query["date"] = {"$gte":start_date}
    if end_date:
        query["date"] = {"$ite":end_date}
    
    e = await Expense_col.find_one(query).to_list(length=100)
    return [{"id": str(e["_id"]),"amount":e["amount"],'category':e["category"], 'description':e['description'], "date":e["date"]}]

@app.get("/expenses/{expense_id}")
async def get_expense_id(expense_id,current_user: str = Depends(verify_token)):
    e = await Expense_col.find_one({"_id":expense_id, "user": current_user})
    if not e:
        raise HTTPException(status_code=400)
    return [{"id": str(e["_id"]),"amount":e["amount"],'category':e["category"], 'description':e['description'], "date":e["date"]}]

@app.put("/expenses/{expense_id}")
async def update_expense(expense_id,request:Expense, current_user: str = Depends(verify_token)):
    e = await Expense_col.find_one({"_id":expense_id, "user": current_user})
    if not e:
        raise HTTPException(status_code=400)
    
    u_e = {'$set':request.dict()}
    await Expense_col.update_one({'_id': e['_id']},u_e)
    
    return [{"id": str(e["_id"]),"amount":u_e["amount"],'category':u_e["category"], 'description':u_e['description'], "date":u_e["date"]}]
    
    
@app.delete("/expenses/{expense_id}")
async def delete_expense(expense_id, current_user: str = Depends(verify_token)):
    e = await Expense_col.find_one({"_id":expense_id, "user": current_user})
    if not e:
        raise HTTPException(status_code=400)
    await Expense_col.delete_one({"_id": e['_id']})
    
    return {"message": "Deleted successfully"}