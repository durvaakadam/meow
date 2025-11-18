# backend/api/routers/upload.py

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from pathlib import Path
import tempfile
import asyncio
import pdfplumber
import fitz
import traceback
import uuid
import json
import zipfile
import os
import shutil
import re
import mimetypes

# services
from backend.services.supabase_service import (
    insert_document_record,
    insert_collection_record,
    get_supabase
)

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
        "bbox": null
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
                "bbox": None
            })
    doc.close()
    return saved

def chunk_text(text: str, chunk_size: int = 1000):
    return [text[i:i+chunk_size] for i in range(0, len(text), chunk_size)]

def dehyphenate_text(text: str) -> str:
    """
    Fix common hyphenation where words split across lines like:
    "multi-\nple" -> "multiple"
    """
    # Replace hyphen at end of line followed by newline + lowercase start
    text = re.sub(r"-\n([a-z0-9])", r"\1", text)
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




# ----------------- main endpoint -----------------
@router.post("/upload-callback")
async def upload_callback(data: UploadCallback):
    # --------- ZIP handling branch ----------

    is_zip = (data.mime_type in ["application/zip", "application/x-zip-compressed"]) or data.filename.lower().endswith(".zip")

    if is_zip:
        zip_filename = Path(data.filename).name                   # e.g. mycase.zip
        zip_stem = Path(data.filename).stem                       # e.g. mycase
        zip_uploaded_path = f"zipUploaded/{zip_filename}"         # final canonical location

        # 1) Download original uploaded ZIP (wherever frontend puts it)
        try:
            zip_bytes = await asyncio.to_thread(
                download_bytes_from_storage,
                DOCUMENTS_BUCKET,
                data.file_path    # may be temp path
            )
        except Exception as e:
            traceback.print_exc()
            raise HTTPException(status_code=500, detail=f"Failed to download ZIP: {e}")

        # 2) Re-upload ZIP to zipUploaded/<filename> (NEEDED to ensure canonical location)
        try:
            await asyncio.to_thread(
                upload_bytes_to_storage,
                DOCUMENTS_BUCKET,
                zip_uploaded_path,
                zip_bytes,
                "application/zip"
            )
        except Exception as e:
            traceback.print_exc()
            raise HTTPException(status_code=500, detail=f"Failed to save ZIP into zipUploaded/: {e}")

        # 3) Save ZIP to temp
        try:
            tmp_zip_fd, tmp_zip_path = tempfile.mkstemp(suffix=".zip")
            os.close(tmp_zip_fd)
            with open(tmp_zip_path, "wb") as f:
                f.write(zip_bytes)
        except Exception as e:
            traceback.print_exc()
            raise HTTPException(status_code=500, detail=f"Failed to write temp ZIP: {e}")

        # 4) Insert collection row with CORRECT path
        try:
            collection = await asyncio.to_thread(insert_collection_record, {
                "name": zip_filename,
                "file_path": zip_uploaded_path,    # FIXED
                "org_id": data.org_id,
                "uploader_id": data.uploader_id
            })
        except Exception as e:
            traceback.print_exc()
            collection = {"id": None, "error": str(e)}

        collection_id = collection.get("id")

        # 5) Extract ZIP
        extracted_docs = []
        temp_extract_dir = tempfile.mkdtemp(prefix="upload_extract_")

        try:
            with zipfile.ZipFile(tmp_zip_path, 'r') as z:
                z.extractall(temp_extract_dir)
        except Exception as e:
            traceback.print_exc()
            shutil.rmtree(temp_extract_dir, ignore_errors=True)
            os.remove(tmp_zip_path)
            raise HTTPException(status_code=500, detail=f"Failed to extract ZIP: {e}")
        
        zip_stem = Path(data.filename).stem 
       
        
        # 6) Walk extracted files
        try:
            for root, dirs, files in os.walk(temp_extract_dir):
                for file in files:

                    # skip nested zips
                    if file.lower().endswith(".zip"):
                        continue

                    local_path = os.path.join(root, file)

                    # relative path inside the zip
                    rel_inside_zip = os.path.relpath(local_path, temp_extract_dir).replace("\\", "/")

                    # FINAL FIXED STORAGE PATH:
                    storage_path = f"uploads/{zip_stem}/{rel_inside_zip}"

                    try:
                        with open(local_path, "rb") as fh:
                            file_bytes = fh.read()

                        mime_type, _ = mimetypes.guess_type(local_path)
                        if not mime_type:
                            mime_type = "application/octet-stream"

                        # upload extracted file
                        await asyncio.to_thread(
                            upload_bytes_to_storage,
                            DOCUMENTS_BUCKET,
                            storage_path,
                            file_bytes,
                            mime_type
                        )

                        # insert document table row
                        doc_record = {
                            "collection_id": collection_id,
                            "file_path": storage_path,
                            "filename": file,
                            "mime_type": mime_type,
                            "file_size": os.path.getsize(local_path),
                            "org_id": data.org_id,
                            "uploader_id": data.uploader_id,
                            "status": "extracted"
                        }
                        inserted = await asyncio.to_thread(insert_document_record, doc_record)
                        extracted_docs.append(inserted)
                        
                        # >>> ADDED: run full parsing only for PDFs
                        # -----------------------------------------------------
                        if file.lower().endswith(".pdf"):

                            # extract text
                            text_content = await asyncio.to_thread(extract_text, local_path)
                            text_content = dehyphenate_text(text_content)
                            text_content = reflow_paragraphs(text_content)
                            text_chunks = await asyncio.to_thread(chunk_text, text_content)

                            # extract images with metadata
                            images_meta_raw = await asyncio.to_thread(extract_images_with_metadata, local_path)

                            # create local processed folder
                            uid = uuid.uuid4().hex
                            local_doc_dir = LOCAL_PROCESSED_ROOT / uid
                            local_doc_dir.mkdir(parents=True, exist_ok=True)

                            # build parsed.json
                            parsed_json = {
                                "file_path": storage_path,
                                "filename": file,
                                "mime_type": mime_type,
                                "chunks": text_chunks,
                                "full_text_preview": text_content[:10000]
                            }
                            parsed_json_path = local_doc_dir / "parsed.json"
                            parsed_json_path.write_text(json.dumps(parsed_json, ensure_ascii=False, indent=2), encoding="utf-8")

                            # handle images locally
                            images_meta = []
                            for img in images_meta_raw:
                                src = Path(img["local_path"])
                                if not src.exists():
                                    continue
                                dst = local_doc_dir / src.name
                                src.replace(dst)
                                images_meta.append({
                                    "page": img["page"],
                                    "image_index": img["image_index"],
                                    "filename": img["filename"],
                                    "local_path": str(dst),
                                    "bbox": img.get("bbox")
                                })

                            images_json_path = local_doc_dir / "images.json"
                            images_json_path.write_text(json.dumps(images_meta, ensure_ascii=False, indent=2), encoding="utf-8")

                            # upload parsed.json to PARSED bucket
                            parsed_remote_name = f"parsed/{uid}.json"
                            await asyncio.to_thread(
                                upload_bytes_to_storage,
                                PARSED_BUCKET,
                                parsed_remote_name,
                                parsed_json_path.read_bytes(),
                                "application/json"
                            )

                            # upload each image to IMAGES bucket
                            uploaded_images_meta = []
                            for img in images_meta:
                                local_img_path = Path(img["local_path"])
                                remote_img_path = f"images/{uid}/{local_img_path.name}"

                                mime, _ = mimetypes.guess_type(local_img_path)
                                if not mime:
                                    mime = "image/png"

                                await asyncio.to_thread(
                                    upload_bytes_to_storage,
                                    IMAGES_BUCKET,
                                    remote_img_path,
                                    local_img_path.read_bytes(),
                                    mime
                                )

                                img["remote_path"] = remote_img_path
                                uploaded_images_meta.append(img)

                            # finally update DB row → parsed
                            inserted["status"] = "parsed"
                            inserted["parsed_json"] = parsed_json
                            inserted["parsed_json_remote_path"] = parsed_remote_name
                            inserted["parsed_json_local_path"] = str(parsed_json_path)
                            inserted["images"] = uploaded_images_meta
                            inserted["images_local_path"] = str(local_doc_dir)

                        # >>> END PDF PARSING ADDITION
                        # -----------------------------------------------------

                    except Exception as e:
                        traceback.print_exc()
                        extracted_docs.append({
                            "file": storage_path,
                            "local_path": local_path,
                            "error": str(e)
                        })

        finally:
            shutil.rmtree(temp_extract_dir, ignore_errors=True)
            try: os.remove(tmp_zip_path)
            except: pass

        return {
            "status": "ok",
            "type": "zip",
            "collection": collection,
            "documents": extracted_docs
        }


    # --------- NON-ZIP (assume single file, e.g., PDF) ----------
    # 1) Download file bytes from supabase storage
    try:
        file_bytes = await asyncio.to_thread(download_bytes_from_storage, DOCUMENTS_BUCKET, data.file_path)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to download from storage: {e}")

    # 2) write file to temp path (use filename from callback)
    try:
        safe_filename = Path(data.filename).name
        tmp_dir = Path(tempfile.gettempdir())
        tmp_file_path = tmp_dir / safe_filename
        tmp_file_path.write_bytes(file_bytes)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to write temp file: {e}")

    # 3) parse text and extract images (run blocking I/O in threads)
    try:
        text_content = await asyncio.to_thread(extract_text, str(tmp_file_path))
        # optional cleanup of hyphenation and paragraph reflow
        text_content = dehyphenate_text(text_content)
        text_content = reflow_paragraphs(text_content)
        text_chunks = await asyncio.to_thread(chunk_text, text_content)
        extracted_images_meta = await asyncio.to_thread(extract_images_with_metadata, str(tmp_file_path))
    except Exception as e:
        traceback.print_exc()
        try:
            tmp_file_path.unlink(missing_ok=True)
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=f"Error parsing file: {e}")

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
        "full_text_preview": text_content[:10000]
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
            "bbox": img.get("bbox")
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
        parsed_remote_name = None

    # upload images and collect remote paths
    uploaded_images_meta = []
    for img in images_meta:
        local_path = Path(img["local_path"])
        remote_name = f"images/{uid}/{local_path.name}"
        try:
            content = local_path.read_bytes()
            # guess content type by file suffix
            mime_type, _ = mimetypes.guess_type(local_path)
            if not mime_type:
                mime_type = "image/png"
            await asyncio.to_thread(upload_bytes_to_storage, IMAGES_BUCKET, remote_name, content, mime_type)
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

    # Clean up temp file if desired
    try:
        tmp_file_path.unlink(missing_ok=True)
    except Exception:
        pass

    return {"status": "ok", "document": inserted, "local_folder": str(local_doc_dir)}
