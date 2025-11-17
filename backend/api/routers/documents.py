from fastapi import APIRouter, UploadFile, File
from pydantic import BaseModel

router = APIRouter()

class DocumentMetadata(BaseModel):
    filename: str
    size: int
    mime_type: str

# In-memory documents storage for demo
documents_db = []

@router.post("/upload")
async def upload_document(file: UploadFile = File(...)):
    """Upload a document"""
    contents = await file.read()
    
    doc = {
        "id": len(documents_db) + 1,
        "filename": file.filename,
        "size": len(contents),
        "mime_type": file.content_type
    }
    
    documents_db.append(doc)
    
    return {
        "status": "success",
        "document_id": doc["id"],
        "filename": file.filename,
        "size": len(contents)
    }

@router.get("/")
async def list_documents():
    """List all documents"""
    return {"documents": documents_db, "total": len(documents_db)}




@router.post("/upload-and-parse")
async def upload_and_parse(file: UploadFile = File(...)):
    # Step 1: Read uploaded file
    file_bytes = await file.read()
    
    # Step 2: Upload to Supabase
    supabase_path = upload_pdf_to_supabase(file_bytes, file.filename)
    
    # Step 3: Parse PDF directly from uploaded bytes
    pages = parse_pdf_from_bytes(file_bytes)
    
    return {
        "supabase_path": supabase_path,
        "pages": pages,
        "page_count": len(pages)
    }
