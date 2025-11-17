# backend/services/supabase_storage.py
from pathlib import Path
import tempfile
import os
from .supabase_service import get_supabase  # uses your existing get_supabase

def upload_bytes_to_storage(bucket: str, remote_path: str, file_bytes: bytes, content_type: str = "application/pdf"):
    client = get_supabase()
    try:
        # Newer supabase SDKs allow upload of bytes via `upload` with file-like or bytes.
        res = client.storage.from_(bucket).upload(remote_path, file_bytes, content_type=content_type)
        return res
    except Exception:
        # fallback: write to temp file then upload by path
        tmp = Path(tempfile.gettempdir()) / ("tmp_" + os.path.basename(remote_path))
        tmp.write_bytes(file_bytes)
        res = client.storage.from_(bucket).upload(remote_path, str(tmp))
        try:
            tmp.unlink()
        except Exception:
            pass
        return res

def download_bytes_from_storage(bucket: str, remote_path: str) -> bytes:
    client = get_supabase()
    res = client.storage.from_(bucket).download(remote_path)
    # handle several SDK return shapes
    if isinstance(res, (bytes, bytearray)):
        return bytes(res)
    if isinstance(res, dict) and res.get("data"):
        return bytes(res["data"])
    if hasattr(res, "content"):
        return res.content
    raise RuntimeError("Unexpected response from Supabase download")
