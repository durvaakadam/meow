from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from backend.core.config import get_settings
from backend.api.routers import collections, documents, search, chat, upload
from backend.api.routers.parse_marker import router as parse_marker_router

settings = get_settings()

app = FastAPI(
    title="VICTOR API",
    description="RAG-powered PDF search system",
    version="1.0.0"
)

# CORS: allow your frontend origins in development
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(collections.router, prefix="/api/collections", tags=["Collections"])
app.include_router(documents.router, prefix="/api/documents", tags=["Documents"])
app.include_router(upload.router, prefix="/api/upload", tags=["Upload"])
# NOTE: removed the duplicate include that used prefix=""
# If you want upload-callback available at root (/upload-callback),
# either add a small separate router with that single path, or change the client to call /api/upload/upload-callback.

app.include_router(search.router, prefix="/api/search", tags=["Search"])
app.include_router(chat.router, prefix="/api/chat", tags=["Chat"])
app.include_router(parse_marker_router, prefix="/api/marker", tags=["Marker"])

@app.get("/")
async def root():
    return {"message": "VICTOR API is running"}

@app.get("/health")
async def health():
    return {"status": "healthy"}
