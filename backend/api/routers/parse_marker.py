# backend/api/routers/parse_marker.py
from fastapi import APIRouter, HTTPException, BackgroundTasks
import asyncio
import subprocess
import tempfile
from pathlib import Path
import json
import uuid
from backend.services.supabase_storage import download_bytes_from_storage, upload_bytes_to_storage
from backend.services.supabase_service import insert_document_record  # optional: to update record
import fitz  # PyMuPDF

router = APIRouter()

DOCUMENTS_BUCKET = "documents"        # or use env var
IMAGES_BUCKET = "documents-images"    # or use env var

async def run_marker_and_get_json(pdf_path: Path, output_format: str = "json", use_llm: bool = False):
    out_dir = pdf_path.parent / "marker_out"
    out_dir.mkdir(exist_ok=True)
    cmd = ["marker_single", str(pdf_path), "--output_format", output_format, "--output_dir", str(out_dir)]
    if use_llm:
        cmd += ["--use_llm"]
    # run in thread to avoid blocking
    def _run():
        r = subprocess.run(cmd, capture_output=True, text=True)
        return r
    result = await asyncio.to_thread(_run)
    if result.returncode != 0:
        raise RuntimeError(f"Marker failed: {result.stderr[:1000]}")
    # Find first json file
    json_files = list(out_dir.glob("*.json"))
    if not json_files:
        # fall back: read all text outputs
        outputs = {}
        for p in out_dir.iterdir():
            if p.is_file():
                outputs[p.name] = p.read_text(encoding="utf-8", errors="ignore")
        return {"outputs": outputs}
    # parse json
    text = json_files[0].read_text(encoding="utf-8")
    return json.loads(text)

def extract_and_save_figures(pdf_path: Path, marker_json: dict, temp_dir: Path):
    """
    Use marker_json to find figure blocks and crop/save them.
    Returns list of dicts: {page, bbox, filename, remote_path}
    """
    saved = []
    doc = fitz.open(str(pdf_path))
    # Marker JSON shape: top-level 'blocks' (or similar). Try to find figure blocks robustly.
    blocks = marker_json.get("blocks") or marker_json.get("content") or marker_json.get("elements") or []
    for block in blocks:
        # Marker may use type or block_type; check both
        if block.get("type") == "figure" or block.get("block_type") == "figure":
            page_num = block.get("page", 1)
            bbox = block.get("bbox")  # expected [x0, y0, x1, y1]
            if not bbox:
                continue
            # fitz pages are 0-indexed
            page = doc[page_num - 1]
            rect = fitz.Rect(*bbox)
            pix = page.get_pixmap(clip=rect, dpi=150)
            fname = f"figure_p{page_num}_{uuid.uuid4().hex}.png"
            out_path = temp_dir / fname
            pix.save(str(out_path))
            saved.append({
                "page": page_num,
                "bbox": bbox,
                "filename": fname,
                "local_path": str(out_path)
            })
    doc.close()
    return saved

async def upload_extracted_images(saved_list, images_bucket=IMAGES_BUCKET):
    results = []
    for item in saved_list:
        local_path = Path(item["local_path"])
        remote_name = f"{local_path.name}"
        content = local_path.read_bytes()
        # upload blocking -> run in thread
        def _upload():
            return upload_bytes_to_storage(images_bucket, remote_name, content, content_type="image/png")
        try:
            res = await asyncio.to_thread(_upload)
            results.append({"local": str(local_path), "remote": remote_name, "upload_res": str(res)})
        except Exception as e:
            results.append({"local": str(local_path), "error": str(e)})
    return results

@router.post("/parse-and-extract")
async def parse_and_extract(remote_path: str, background: bool = False, use_llm: bool = False, bg: BackgroundTasks = None):
    """
    Args:
      remote_path: path in storage you saved earlier (e.g. uuid_filename.pdf)
      background: if true, processing runs but returns 202 immediately (not implemented full queue here)
    """
    # 1. Download pdf bytes
    try:
        pdf_bytes = await asyncio.to_thread(download_bytes_from_storage, DOCUMENTS_BUCKET, remote_path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed download: {e}")

    # write to temp file
    td = Path(tempfile.mkdtemp())
    pdf_path = td / "input.pdf"
    pdf_path.write_bytes(pdf_bytes)

    # Optionally run in background tasks (simple)
    async def _process():
        try:
            marker_json = await run_marker_and_get_json(pdf_path, output_format="json", use_llm=use_llm)
            saved = extract_and_save_figures(pdf_path, marker_json, td)
            upload_results = await upload_extracted_images(saved)
            # Option: update DB record with parsed content path / images metadata
            try:
                insert_document_record({
                    "file_path": remote_path,
                    "parsed_json": marker_json,
                    "extracted_images": upload_results,
                    "status": "parsed"
                })
            except Exception:
                # optional: ignore DB update failure
                pass
        finally:
            # cleanup local temp files (optional)
            for p in td.iterdir():
                try:
                    p.unlink()
                except Exception:
                    pass
            try:
                td.rmdir()
            except Exception:
                pass
        return {"status": "done"}

    if background:
        # schedule background processing quickly and return 202
        bg.add_task(asyncio.create_task, _process())
        return {"status": "accepted", "remote_path": remote_path}
    else:
        result = await _process()
        return {"status": "ok", "remote_path": remote_path, "result": result}
