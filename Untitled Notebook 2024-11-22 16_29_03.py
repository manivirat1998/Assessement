# Databricks notebook source
%pip install fastapi
%pip install pydantic
%pip install requests
%pip install sqlalchemy

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import requests
import sqlalchemy
from sqlalchemy import create_engine, Column, Integer, String, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# FastAPI app initialization
app = FastAPI()

# Database setup with SQLAlchemy
DATABASE_URL = "sqlite:///./test.db"
Base = declarative_base()

class Joke(Base):
    __tablename__ = 'jokes'
    
    id = Column(Integer, primary_key=True, index=True)
    category = Column(String, index=True)
    type = Column(String)
    joke = Column(String, nullable=True)  # For "single" type
    setup = Column(String, nullable=True)  # For "twopart" type
    delivery = Column(String, nullable=True)  # For "twopart" type
    nsfw = Column(Boolean, default=False)
    political = Column(Boolean, default=False)
    sexist = Column(Boolean, default=False)
    safe = Column(Boolean, default=False)
    lang = Column(String)

# Set up SQLite Database
engine = create_engine(DATABASE_URL)
Base.metadata.create_all(bind=engine)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Pydantic model for joke schema
class JokeSchema(BaseModel):
    category: str
    type: str
    joke: str = None
    setup: str = None
    delivery: str = None
    nsfw: bool
    political: bool
    sexist: bool
    safe: bool
    lang: str

    class Config:
        orm_mode = True

# Function to get jokes from JokeAPI
def fetch_jokes_from_api():
    url = "https://v2.jokeapi.dev/joke/Any?type=twopart&amount=100"
    response = requests.get(url)
    
    if response.status_code != 200:
        raise HTTPException(status_code=500, detail="Failed to fetch jokes")
    
    jokes_data = response.json()
    
    jokes = []
    
    # Handling both single and twopart joke types
    for joke in jokes_data['jokes']:
        processed_joke = {
            'category': joke['category'],
            'type': joke['type'],
            'nsfw': joke['flags'].get('nsfw', False),
            'political': joke['flags'].get('political', False),
            'sexist': joke['flags'].get('sexist', False),
            'safe': joke['flags'].get('safe', False),
            'lang': joke['lang']
        }
        
        if joke['type'] == 'single':
            processed_joke['joke'] = joke['joke']
        elif joke['type'] == 'twopart':
            processed_joke['setup'] = joke['setup']
            processed_joke['delivery'] = joke['delivery']
        
        jokes.append(processed_joke)
    
    return jokes

# Endpoint to fetch jokes from JokeAPI and store them in the database
@app.post("/fetch_jokes/")
async def fetch_and_store_jokes():
    jokes = fetch_jokes_from_api()

    # Store jokes in database
    db = SessionLocal()
    for joke in jokes:
        db_joke = Joke(
            category=joke['category'],
            type=joke['type'],
            joke=joke.get('joke'),
            setup=joke.get('setup'),
            delivery=joke.get('delivery'),
            nsfw=joke['nsfw'],
            political=joke['political'],
            sexist=joke['sexist'],
            safe=joke['safe'],
            lang=joke['lang']
        )
        db.add(db_joke)
    db.commit()
    db.close()

    return {"message": "Jokes fetched and stored successfully"}

# Endpoint to get stored jokes from the database
@app.get("/jokes/")
async def get_jokes(skip: int = 0, limit: int = 10):
    db = SessionLocal()
    jokes = db.query(Joke).offset(skip).limit(limit).all()
    db.close()
    
    return jokes

