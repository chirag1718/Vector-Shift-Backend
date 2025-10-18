from fastapi import FastAPI, Form
from fastapi.middleware.cors import CORSMiddleware
import os
from dotenv import load_dotenv

# Load variables from .env file
load_dotenv()

app = FastAPI()

# CORS setup
app.add_middleware(
    CORSMiddleware,
    allow_origins=[os.getenv("FRONTEND_URL", "http://localhost:3000")], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get('/')
def read_root():
    return {'Ping': 'Pong'}

@app.get('/pipelines/parse')
def parse_pipeline(pipeline: str = Form(...)):
    return {'status': 'parsed'}
