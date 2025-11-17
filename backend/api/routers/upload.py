# backend/api/routers/upload.py

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from backend.services.supabase_service import insert_document_record
from backend.services.supabase_storage import download_bytes_from_storage, upload_bytes_to_storage
from pathlib import Path
import tempfile
import asyncio
import pdfplumber
import fitz
import traceback
import uuid
import json
import os

router = APIRouter()

class UploadCallback(BaseModel):
    file_path: str         # remote storage path (supabase)
    filename: str
    mime_type: str
    file_size: int
    org_id: str = ""
    uploader_id: str = ""

# Config (make env vars if you prefer)
DOCUMENTS_BUCKET = "documents"
PARSED_BUCKET = "documents-parsed"
IMAGES_BUCKET = "documents-images"
LOCAL_PROCESSED_ROOT = Path("backend/data/processed")

# ----------------- sync helper functions (run in threads) -----------------
def extract_text(pdf_path: str) -> str:
    text_content = ""
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            txt = page.extract_text()
            if txt:
                text_content += txt + "\n"
    return text_content

def extract_images_with_metadata(pdf_path: str):
    """
    Extract actual embedded images from the PDF using PyMuPDF and return a list of metadata:
    [
      {
        "page": int,
        "image_index": int,
        "filename": "xxx.png",
        "local_path": "/tmp/xxx.png",
        "bbox": null  # if you want bbox, Marker provides it; pure extraction gives raw images
      },
      ...
    ]
    """
    doc = fitz.open(pdf_path)
    saved = []
    for page_num, page in enumerate(doc):
        for img_index, img in enumerate(page.get_images(full=True)):
            xref = img[0]
            base_image = doc.extract_image(xref)
            image_bytes = base_image["image"]
            image_ext = base_image.get("ext", "png")
            fname = f"{Path(pdf_path).stem}_p{page_num+1}_img{img_index}.{image_ext}"
            out_path = Path(tempfile.gettempdir()) / fname
            with open(out_path, "wb") as f:
                f.write(image_bytes)
            saved.append({
                "page": page_num + 1,
                "image_index": img_index,
                "filename": fname,
                "local_path": str(out_path),
                # no bbox here; if you run Marker to get bbox, include it in parsed JSON and correlate.
                "bbox": None
            })
    doc.close()
    return saved

def chunk_text(text: str, chunk_size: int = 1000):
    return [text[i:i+chunk_size] for i in range(0, len(text), chunk_size)]

# ----------------- main endpoint -----------------
@router.post("/upload-callback")
async def upload_callback(data: UploadCallback):
    """
    Flow:
      1) download original PDF from Supabase storage
      2) write to a temporary PDF file
      3) extract text & images (in thread)
      4) create local processed folder and save parsed.json and images.json
      5) upload parsed.json to PARSED_BUCKET and upload images to IMAGES_BUCKET
      6) insert DB row with both local and remote paths/metadata
    """
    # 1) Download pdf bytes from supabase storage
    try:
        pdf_bytes = await asyncio.to_thread(download_bytes_from_storage, DOCUMENTS_BUCKET, data.file_path)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to download from storage: {e}")

    # 2) write pdf to temp file
    try:
        safe_filename = Path(data.filename).name
        tmp_dir = Path(tempfile.gettempdir())
        tmp_pdf_path = tmp_dir / safe_filename
        tmp_pdf_path.write_bytes(pdf_bytes)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to write temp PDF: {e}")

    # 3) parse text and extract images (run blocking I/O in threads)
    try:
        text_content = await asyncio.to_thread(extract_text, str(tmp_pdf_path))
        text_chunks = await asyncio.to_thread(chunk_text, text_content)
        extracted_images_meta = await asyncio.to_thread(extract_images_with_metadata, str(tmp_pdf_path))
    except Exception as e:
        traceback.print_exc()
        try:
            tmp_pdf_path.unlink(missing_ok=True)
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=f"Error parsing PDF: {e}")

    # 4) prepare local processed folder
    uid = uuid.uuid4().hex
    local_doc_dir = LOCAL_PROCESSED_ROOT / uid
    local_doc_dir.mkdir(parents=True, exist_ok=True)

    # Build parsed JSON object (you may instead run Marker and use its JSON here)
    parsed_json = {
        "file_path": data.file_path,
        "filename": data.filename,
        "mime_type": data.mime_type,
        "chunks": text_chunks,
        "full_text_preview": text_content[:10000]  # optional
        # add extra metadata (pages, images, marker blocks) if you run Marker separately
    }

    # Save parsed.json locally
    parsed_json_path = local_doc_dir / "parsed.json"
    parsed_json_path.write_text(json.dumps(parsed_json, ensure_ascii=False, indent=2), encoding="utf-8")

    # Save images metadata locally (images.json) and move image files into local folder
    images_meta = []
    for img in extracted_images_meta:
        src = Path(img["local_path"])
        if not src.exists():
            continue
        dst = local_doc_dir / src.name
        src.replace(dst)  # move from tempdir into local folder
        img_entry = {
            "page": img["page"],
            "image_index": img["image_index"],
            "filename": img["filename"],
            "local_path": str(dst),
            "bbox": img.get("bbox")  # None unless you have bbox from Marker
        }
        images_meta.append(img_entry)

    images_json_path = local_doc_dir / "images.json"
    images_json_path.write_text(json.dumps(images_meta, ensure_ascii=False, indent=2), encoding="utf-8")

    # 5) Upload parsed.json to Supabase Storage (PARSED_BUCKET) and upload images to IMAGES_BUCKET
    parsed_remote_name = f"parsed/{uid}.json"
    try:
        parsed_bytes = parsed_json_path.read_bytes()
        await asyncio.to_thread(upload_bytes_to_storage, PARSED_BUCKET, parsed_remote_name, parsed_bytes, "application/json")
    except Exception as e:
        traceback.print_exc()
        # not fatal — continue but record error
        parsed_remote_name = None

    # upload images and collect remote paths
    uploaded_images_meta = []
    for img in images_meta:
        local_path = Path(img["local_path"])
        remote_name = f"images/{uid}/{local_path.name}"
        try:
            content = local_path.read_bytes()
            await asyncio.to_thread(upload_bytes_to_storage, IMAGES_BUCKET, remote_name, content, "image/png")
            uploaded_images_meta.append({
                "page": img["page"],
                "image_index": img["image_index"],
                "filename": img["filename"],
                "local_path": str(local_path),
                "remote_path": remote_name,
                "bbox": img.get("bbox")
            })
        except Exception as e:
            traceback.print_exc()
            uploaded_images_meta.append({
                "page": img["page"],
                "image_index": img["image_index"],
                "filename": img["filename"],
                "local_path": str(local_path),
                "remote_path": None,
                "error": str(e)
            })

    # 6) Build DB record and insert
    record = {
        "file_path": data.file_path,
        "filename": data.filename,
        "mime_type": data.mime_type,
        "file_size": data.file_size,
        "org_id": data.org_id,
        "uploader_id": data.uploader_id,
        "status": "parsed",
        "parsed_json": parsed_json,                       # JSONB column
        "parsed_json_remote_path": parsed_remote_name,    # text column
        "parsed_json_local_path": str(parsed_json_path),  # text column
        "images": uploaded_images_meta,                    # JSONB column
        "images_local_path": str(local_doc_dir)           # text column
    }

    try:
        inserted = await asyncio.to_thread(insert_document_record, record)
    except Exception:
        traceback.print_exc()
        record["db_error"] = "insert failed"
        inserted = record

    # Clean up temp pdf if desired
    try:
        tmp_pdf_path.unlink(missing_ok=True)
    except Exception:
        pass

    return {"status": "ok", "document": inserted, "local_folder": str(local_doc_dir)}


def dehyphenate_text(text: str) -> str:
    """
    Fix common hyphenation where words split across lines like:
    "multi-\nple" -> "multiple"
    """
    # Replace hyphen at end of line followed by newline + lowercase start
    text = re.sub(r"-\n([a-z0-9])", r"\1", text)  # join hyphenated words
    # Join words broken by newline (simple heuristic)
    text = re.sub(r"([a-z0-9])\n([a-z])", r"\1 \2", text)
    return text

def reflow_paragraphs(text: str, maxlen: int = 1000) -> str:
    """
    Collapse repeated newlines, keep paragraph breaks, produce paragraph strings.
    Useful to create chunks for embeddings later.
    """
    # unify CRLF
    text = text.replace("\r\n", "\n")
    # Collapse more than 2 newlines -> paragraph break
    text = re.sub(r'\n{2,}', '\n\n', text)
    # For single-line breaks inside paragraphs, replace with space
    paragraphs = []
    for para in text.split("\n\n"):
        # remove stray newlines inside paragraph
        p = " ".join(line.strip() for line in para.splitlines() if line.strip())
        paragraphs.append(p.strip())
    return "\n\n".join(paragraphs)