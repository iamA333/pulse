from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from app.api import router as api_router


class HealthResponse(BaseModel):
    status: str


app = FastAPI(title="Pulse API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.include_router(api_router)


@app.get("/", response_model=HealthResponse)
async def root():
    return HealthResponse(status="ok")
