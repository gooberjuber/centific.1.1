from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes import app as routes

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(routes, prefix="/api/v1")


@app.get("/")
async def root():
    return {"message": "Hello World"}

