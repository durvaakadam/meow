# backend/services/supabase_service.py

from supabase import create_client
import os
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables from .env file in backend directory
backend_dir = Path(__file__).parent.parent
env_file = backend_dir / ".env"
print(f"DEBUG: Looking for .env file at: {env_file}")
print(f"DEBUG: .env file exists: {env_file.exists()}")
load_dotenv(env_file)

# Lazy initialization - only create client when needed
_supabase_client = None

def get_supabase():
    global _supabase_client
    if _supabase_client is None:
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        print(f"DEBUG: SUPABASE_URL = {url}")
        print(f"DEBUG: SUPABASE_SERVICE_ROLE_KEY = {key[:20] if key else 'None'}...")
        if url and key:
            _supabase_client = create_client(url, key)
            print("DEBUG: Supabase client initialized successfully")
        else:
            raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY environment variables are required")
    return _supabase_client

def insert_document_record(data: dict):
    """Insert document record into Supabase"""
    try:
        print(f"DEBUG: Inserting document: {data}")
        supabase = get_supabase()
        result = (
            supabase.table("documents")
            .insert(data)
            .execute()
        )
        print(f"DEBUG: Insert result: {result}")
        print(f"DEBUG: Insert result data: {result.data}")
        return result.data[0] if result.data else data
    except Exception as e:
        print(f"ERROR inserting document: {e}")
        import traceback
        traceback.print_exc()
        # Return the data as-is if insertion fails
        return data

def insert_collection_record(data: dict):
    """Insert collection record into Supabase"""
    try:
        supabase = get_supabase()
        result = (
            supabase.table("collections")
            .insert(data)
            .execute()
        )
        return result.data[0] if result.data else data
    except Exception as e:
        print(f"ERROR inserting collection: {e}")
        return data
    

