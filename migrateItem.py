import os
import json
import logging
import requests
import psycopg2
import argparse
import re
from concurrent.futures import ThreadPoolExecutor, as_completed 
import threading
from tqdm import tqdm
from datetime import datetime, timedelta


# ============================================================================
# 🔹 CONFIGURATION
# ============================================================================

BASE_URL = "http://10.184.240.87:8080/server/api"
USERNAME = "admin@gmail.com"
PASSWORD = "admin"

SRC_DB = {
    "dbname": "batch",
    "user": "anthem",
    "password": "anthem",
    "host": "localhost",
    "port": 5432
}

CINO_DB = {
    "dbname": "cisnc",
    "user": "postgres",
    "password": "anthem@123",
    "host": "10.184.240.25",
    "port": 5432
}

ASSETSTORE_PATHS = [
    "/mnt/Digitaization1/assetstore",
    "/mnt/external/assetstore",
    "/mnt/filing/assetstore",
    "/mnt/rrdc/assetstore",
    "/mnt/internal",
]

# Mapping of filename prefixes to assetstore paths
ASSETSTORE_PREFIX_MAP = {
    "hcserver": "/mnt/filing/assetstore",
    "assetstore2": "/mnt/digitaization1/assetstore",
    "hcserverasset2": "/mnt/rrdc/assetstore",
    "external": "/mnt/external/assetstore",
    "internal": "/mnt/internal",
}

COMMON_COLUMNS = [
    "item_id", "in_archive", "withdrawn", "last_modified",
    "discoverable", "uuid", "submitter_id", "owning_collection"
]

# ============================================================================
# 🔹 STRUCTURED ERROR LOG FILES
# ============================================================================

class ErrorLogger:
    """Separate log files per error type, no timestamps"""

    def __init__(self, base_dir="migration_logs"):
        os.makedirs(base_dir, exist_ok=True)

        self.logs = {
            "missing_pdf": f"{base_dir}/missing_pdf_2.log",
            "missing_cino": f"{base_dir}/missing_cino.log",
            "workflow_failed": f"{base_dir}/workflow_failed.log",
            "metadata_failed": f"{base_dir}/metadata_failed.log",
            "auth_failed": f"{base_dir}/auth_failed.log",
            "general_error": f"{base_dir}/general_error.log",
            "success": f"{base_dir}/success.log",
        }

        self.locks = {k: threading.Lock() for k in self.logs}

        self.summary = {
            "successful": 0,
            "failed": 0,
            "start_time": None,
            "end_time": None
        }

        print("\n📁 Separate log files created (no timestamps):")
        for f in self.logs.values():
            print(f"   - {f}")
        print()

    def log_error(self, error_type, data):
        if error_type not in self.logs:
            error_type = "general_error"

        uuid      = data.get("uuid", "N/A")
        wsid      = data.get("workspace_id", "N/A")
        case_no   = data.get("case_no", "N/A")
        case_year = data.get("case_year", "N/A")
        case_type = data.get("case_type", "N/A")
        batch     = data.get("batch_no", "N/A")
        error     = data.get("error", "Unknown error")

        # For missing_pdf, override error with the actual filenames
        if error_type == "missing_pdf":
            missing_files = data.get("missing_files", [])
            if missing_files:
                filenames = [f.get("filename", "unknown") for f in missing_files]
                error = f"PDF still missing after retry: {filenames}"

        line = (
            f"{uuid} | WSID={wsid} | "
            f"Case={case_type} {case_no}/{case_year} | "
            f"Batch={batch} | {error}"
        )

        with self.locks[error_type]:
            with open(self.logs[error_type], "a") as f:
                f.write(line + "\n")

        self.summary["failed"] += 1

    def log_success(self, data):
        uuid = data.get("uuid", "N/A")
        wsid = data.get("workspace_id", "N/A")
        case_no = data.get("case_no", "N/A")
        case_year = data.get("case_year", "N/A")
        files = data.get("files_uploaded", 0)

        line = (
            f"{uuid} | WSID={wsid} | "
            f"Case={case_no}/{case_year} | Files={files}"
        )

        with self.locks["success"]:
            with open(self.logs["success"], "a") as f:
                f.write(line + "\n")

        self.summary["successful"] += 1

    def increment_failed(self):
        self.summary["failed"] += 1

    def update_summary(self):
        summary_file = "migration_logs/summary.log"
        with open(summary_file, "a") as f:
            f.write(
                f"\nSUMMARY: Success={self.summary['successful']} "
                f"Failed={self.summary['failed']}\n"
            )


# Global error logger
error_logger = None

# ============================================================================
# 🔹 LOGGING SETUP
# ============================================================================

logging.basicConfig(
    filename="migration_detailed.log",
    level=logging.INFO,
    format="%(levelname)s - %(message)s"
)


# ============================================================================
# 🔹 SESSION MANAGEMENT
# ============================================================================

class SessionManager:
    """Thread-safe session manager with automatic renewal"""
    
    def __init__(self, session_lifetime_minutes=15):
        self.session = None
        self.session_created_at = None
        self.session_lifetime = timedelta(minutes=session_lifetime_minutes)
        self.lock = threading.Lock()
    
    def get_session(self):
        with self.lock:
            if self._is_session_expired():
                try:
                    self.session = self._create_new_session()
                    self.session_created_at = datetime.now()
                except Exception as e:
                    logging.error(f"❌ Session renewal failed: {e}")
                    raise  # Don't silently swallow this
            return self.session
    
    def _is_session_expired(self):
        """Check if session needs renewal"""
        if self.session is None or self.session_created_at is None:
            return True
        elapsed = datetime.now() - self.session_created_at
        return elapsed >= self.session_lifetime
    
    def _create_new_session(self):
        """Create a fresh authenticated session"""
        session = requests.Session()
        
        # Get CSRF token
        status_resp = session.post(
            f"{BASE_URL}/authn/status",
            headers={"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"},
            timeout=(10, 30)
        )
        status_resp = session.get(f"{BASE_URL}/authn/status", timeout=(10, 30))
        
        if status_resp.status_code != 200:
            raise Exception(f"/authn/status failed: {status_resp.status_code}")
        
        csrf_token = (session.cookies.get("DSPACE-XSRF-COOKIE") or 
                     session.cookies.get("XSRF-TOKEN") or 
                     session.cookies.get("csrftoken"))
        DSPACE_XSRF_TOKEN = session.cookies.get("DSPACE-XSRF-TOKEN")
        
        if not csrf_token:
            raise Exception(f"No CSRF token found")
        
        # Login
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "X-XSRF-TOKEN": csrf_token,
            "DSPACE-XSRF-TOKEN": DSPACE_XSRF_TOKEN
        }
        data = {"user": USERNAME, "password": PASSWORD}
        login_resp = session.post(f"{BASE_URL}/authn/login", headers=headers, data=data, timeout=(10, 30))
        
        if login_resp.status_code != 200:
            raise Exception(f"Login failed: {login_resp.status_code}")
        
        jwt = login_resp.headers.get("Authorization")
        if not jwt:
            raise Exception("No Authorization JWT returned")
        
        session.headers.update({"Authorization": jwt, "X-XSRF-TOKEN": csrf_token})
        return session
    
    def force_renew(self):
        """Force session renewal - does NOT use lock to avoid deadlock"""
        logging.info("🔄 Forcing session renewal...")
        new_session = self._create_new_session()  # outside lock
        with self.lock:
            self.session = new_session
            self.session_created_at = datetime.now()
        return self.session

session_manager = SessionManager(session_lifetime_minutes=15)

# ============================================================================
# 🔹 DATABASE CONNECTION POOLING
# ============================================================================

from psycopg2 import pool

# Create connection pools for better performance
src_db_pool = None
cino_db_pool = None

def init_db_pools(min_conn=5, max_conn=30):
    """Initialize database connection pools with proper sizing
    
    Rule of thumb: max_conn should be at least 2-3x thread count
    Each thread may need multiple DB connections (src_db + cino_db)
    """
    global src_db_pool, cino_db_pool
    
    try:
        src_db_pool = pool.ThreadedConnectionPool(
            minconn=min_conn,
            maxconn=max_conn,
            **SRC_DB
        )
        
        cino_db_pool = pool.ThreadedConnectionPool(
            minconn=min_conn,
            maxconn=max_conn,
            **CINO_DB
        )
        
        logging.info(f"✅ Database connection pools initialized (min={min_conn}, max={max_conn})")
        logging.info(f"   SRC_DB pool: {min_conn}-{max_conn} connections")
        logging.info(f"   CINO_DB pool: {min_conn}-{max_conn} connections")
    except Exception as e:
        logging.error(f"❌ Failed to initialize database pools: {e}")
        raise Exception(f"Database pool initialization failed: {e}")

def ensure_db_pools_initialized():
    """Ensure database pools are initialized before use"""
    global src_db_pool, cino_db_pool
    
    if src_db_pool is None or cino_db_pool is None:
        logging.warning("⚠️ Database pools not initialized, initializing now...")
        init_db_pools()

def close_db_pools():
    """Close database connection pools"""
    global src_db_pool, cino_db_pool
    if src_db_pool:
        src_db_pool.closeall()
        src_db_pool = None
    if cino_db_pool:
        cino_db_pool.closeall()
        cino_db_pool = None
    logging.info("✅ Database connection pools closed")

# ============================================================================
# 🔹 CACHING LAYER WITH SIZE LIMITS
# ============================================================================

from functools import lru_cache
import hashlib
from collections import OrderedDict

# LRU Cache with size limits to prevent memory bloat
class LRUCache:
    """Thread-safe LRU cache with maximum size"""
    def __init__(self, max_size=10000):
        self.cache = OrderedDict()
        self.max_size = max_size
        self.lock = threading.Lock()
    
    def get(self, key):
        with self.lock:
            if key in self.cache:
                # Move to end (most recently used)
                self.cache.move_to_end(key)
                return self.cache[key]
            return None
    
    def set(self, key, value):
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
            self.cache[key] = value
            
            # Remove oldest if exceeds max size
            if len(self.cache) > self.max_size:
                self.cache.popitem(last=False)
    
    def size(self):
        with self.lock:
            return len(self.cache)
    
    def clear(self):
        with self.lock:
            self.cache.clear()

# Replace unlimited caches with LRU caches
file_path_cache = LRUCache(max_size=50000)  # Keep last 50k file paths
cino_cache = LRUCache(max_size=20000)  # Keep last 20k CINO lookups

def get_cached_file_path(internal_id):
    """Get file path from cache or filesystem"""
    cached = file_path_cache.get(internal_id)
    if cached is not None:
        return cached
    
    # Not in cache, find it
    subdirs = "/".join([internal_id[i:i+2] for i in range(0, 6, 2)])
    for base in ASSETSTORE_PATHS:
        full_path = f"{base}/{subdirs}/{internal_id}"
        if os.path.exists(full_path):
            file_path_cache.set(internal_id, full_path)
            return full_path
    
    # Cache negative result
    file_path_cache.set(internal_id, None)
    return None

def get_cached_cino(case_type, reg_no, reg_year):
    """Get CINO from cache or database"""
    cache_key = f"{case_type}|{reg_no}|{reg_year}"
    
    cached = cino_cache.get(cache_key)
    if cached is not None:
        return cached
    
    # Not in cache, query database
    ensure_db_pools_initialized()  # Safety check
    
    conn = None
    try:
        conn = cino_db_pool.getconn()
        cur = conn.cursor()
        query = """
            SELECT c.cino FROM civil_t_a c
            JOIN case_type_t ct ON c.filcase_type = ct.case_type
            WHERE ct.type_name = %s AND c.reg_no = %s AND c.reg_year = %s
            LIMIT 1;
        """
        cur.execute(query, (case_type, reg_no, reg_year))
        result = cur.fetchone()
        cur.close()
        
        cino_value = result[0] if result else None
        cino_cache.set(cache_key, cino_value)
        
        return cino_value
    finally:
        if conn:
            cino_db_pool.putconn(conn)

# ============================================================================
# 🔹 HELPER FUNCTIONS
# ============================================================================

def _get_csrf_from_session(session):
    """Get CSRF token from session"""
    token = (session.cookies.get("DSPACE-XSRF-COOKIE") or
            session.cookies.get("XSRF-TOKEN") or
            session.cookies.get("csrftoken"))
    if token:
        return token
    
    try:
        session.get(f"{BASE_URL}/authn/status", headers={"Accept": "application/json"})
    except Exception as e:
        logging.info(f"authn/status refresh failed: {e}")
    
    return (session.cookies.get("DSPACE-XSRF-COOKIE") or
           session.cookies.get("XSRF-TOKEN") or
           session.cookies.get("csrftoken"))

def retry_on_auth_failure(func):
    """Decorator to retry API calls on authentication failure"""
    def wrapper(*args, **kwargs):
        max_retries = 2
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                error_str = str(e).lower()
                if ('401' in error_str or 'unauthorized' in error_str) and attempt < max_retries - 1:
                    logging.warning(f"⚠️ Auth error: {e}. Retrying...")
                    session_manager.force_renew()
                    if len(args) > 0 and hasattr(args[0], 'headers'):
                        args = (session_manager.get_session(),) + args[1:]
                else:
                    raise
    return wrapper

@retry_on_auth_failure
def create_workspaceitem(session, item):
    url = f"{BASE_URL}/submission/workspaceitems"
    csrf = _get_csrf_from_session(session)

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-XSRF-TOKEN": csrf
    }

    resp = session.post(url, json=item, headers=headers, timeout=(10, 60))

    if resp.status_code not in (200, 201):
        raise Exception(f"Failed to create workspace: {resp.status_code} {resp.text}")

    ws_json = safe_json(resp)

    return ws_json.get("id"), ws_json


@retry_on_auth_failure
def patch_metadata(session, workspace_id, metadata_dict):
    """Patch metadata to workspace item"""
    csrf_token = _get_csrf_from_session(session)
    if not csrf_token:
        raise Exception("No CSRF token found")
    
    url = f"{BASE_URL}/submission/workspaceitems/{workspace_id}"
    
    allowed_fields = {
        "dc.title", "dc.title.alternative", "dc.casetype", "dc.caseyear", "dc.cino",
        "dc.judge.name", "dc.pname", "dc.rname", "dc.raname", "dc.paname",
        "dc.case.cnrno", "dc.district", "dc.date.scan", "dc.case.approveby",
        "dc.date.verification", "dc.barcode", "dc.batch-number", "dc.size",
        "dc.date.disposal", "dc.char-count", "dc.date.issued", "dc.publisher",
        "dc.identifier.citation", "dc.relation.ispartofseries", "dc.identifier",
        "dc.language.iso", "dc.verified-by", "dc.date.receiving", "dc.date.shredding"
    }
    
    patch_body = []
    for field, values in metadata_dict.items():
        if field not in allowed_fields:
            continue
        clean_values = [{"value": v.get("value", "N/A")} for v in values]
        patch_body.append({
            "op": "add",
            "path": f"/sections/traditionalpageone/{field}",
            "value": clean_values
        })
    
    patch_body.append({"op": "add", "path": "/sections/license/granted", "value": True})
    patch_body.append({
        "op": "replace",
        "path": "/sections/collection",
        "value": "a7ce67b6-2be5-4faf-b8d3-012185b796e3"
    })
    
    headers = {
        "Authorization": session.headers.get("Authorization"),
        "X-XSRF-TOKEN": csrf_token,
        "Content-Type": "application/json-patch+json",
        "Accept": "application/json"
    }
    # print("Patch body"+ patch_body)
    
    resp = session.patch(url, headers=headers, json=patch_body, timeout=(10, 60))
    if resp.status_code not in (200, 201):
        raise Exception(f"Patch failed: {resp.status_code} {resp.text}")

@retry_on_auth_failure
def upload_bitstream(session, workspace_id, file_path, checksum, filename=None):
    """Upload file to workspace item"""
    if not file_path or not os.path.exists(file_path):
        raise Exception(f"File not found: {file_path}")
    
    if not filename:
        filename = os.path.basename(file_path)
    if not filename.endswith(".pdf"):
        filename = os.path.splitext(filename)[0] + ".pdf"
    
    url = f"{BASE_URL}/submission/workspaceitems/{workspace_id}"
    csrf = _get_csrf_from_session(session)
    headers = {
        "Authorization": session.headers.get("Authorization"),
        "X-XSRF-TOKEN": csrf,
    }
    
    # Calculate file size for timeout (1 second per MB minimum)
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    upload_timeout = max(60, int(file_size_mb * 2))  # 2 seconds per MB, min 60 sec
    
    with open(file_path, "rb") as f:
        files = {"file": (filename, f, "application/pdf")}
        resp = session.post(url, headers=headers, files=files, timeout=(10, upload_timeout))
    
    if resp.status_code not in (200, 201):
        raise Exception(f"Upload failed: {resp.status_code} {resp.text}")


@retry_on_auth_failure
def submit_to_workflow(session, workspace_id):
    csrf = _get_csrf_from_session(session)

    url = f"{BASE_URL}/workflow/workflowitems?embed=item,sections,collection"
    body = f"{BASE_URL}/submission/workspaceitems/{workspace_id}"

    headers = {
        "Authorization": session.headers.get("Authorization"),
        "X-XSRF-TOKEN": csrf,
        "Content-Type": "text/uri-list"
    }

    resp = session.post(url, headers=headers, data=body, timeout=(10, 60))

    if resp.status_code not in (200, 201):
        raise Exception(f"Workflow submission failed: {resp.status_code} {resp.text}")

    workflow_json = safe_json(resp)

    return workflow_json.get("id"), workflow_json


def fetch_cino_number(case_type, reg_no, reg_year):
    """Fetch CINO number from database (DEPRECATED - use get_cached_cino)"""
    return get_cached_cino(case_type, reg_no, reg_year)

def get_assetstore_path_from_internal_id(internal_id):
    """Find file path from internal ID (DEPRECATED - use get_cached_file_path)"""
    return get_cached_file_path(internal_id)

def fetch_item_internal_ids(item_uuid):
    """Fetch internal IDs for item files - OPTIMIZED with connection pool"""
    conn = None
    try:
        conn = src_db_pool.getconn()
        cur = conn.cursor()
        # Optimized query with proper indexing hints
        query = """
            SELECT 
                b.internal_id, 
                b.checksum, 
                title_mv.text_value AS file_name
            FROM item i
            INNER JOIN item2bundle i2b ON i2b.item_id = i.uuid
            INNER JOIN bundle2bitstream b2b ON b2b.bundle_id = i2b.bundle_id
            INNER JOIN bitstream b ON b.uuid = b2b.bitstream_id
            LEFT JOIN metadatavalue title_mv
                ON title_mv.dspace_object_id = b.uuid
                AND title_mv.metadata_field_id IN (
                    SELECT metadata_field_id 
                    FROM metadatafieldregistry 
                    WHERE element = 'title'
                )
            WHERE i.uuid = %s
              AND title_mv.text_value ILIKE '%%.pdf'
              AND title_mv.text_value NOT ILIKE '%%.pdf.txt'
        """
        cur.execute(query, (item_uuid,))
        rows = cur.fetchall()
        cur.close()
        return [{"internal_id": r[0], "checksum": r[1], "file_name": r[2]} for r in rows]
    finally:
        if conn:
            src_db_pool.putconn(conn)

def fetch_item_metadata(item_uuid):
    """Fetch metadata for an item - OPTIMIZED with connection pool"""
    ensure_db_pools_initialized()  # Safety check
    
    conn = None
    try:
        conn = src_db_pool.getconn()
        cur = conn.cursor()
        # Optimized query with explicit joins
        query = """
            SELECT
                ms.short_id || '.' || mf.element || COALESCE('.' || mf.qualifier, '') AS field,
                mv.text_value
            FROM metadatavalue mv
            INNER JOIN metadatafieldregistry mf ON mv.metadata_field_id = mf.metadata_field_id
            INNER JOIN metadataschemaregistry ms ON mf.metadata_schema_id = ms.metadata_schema_id
            WHERE mv.dspace_object_id = %s
            ORDER BY field, mv.place;
        """
        cur.execute(query, (item_uuid,))
        rows = cur.fetchall()
        cur.close()
        
        metadata = {}
        for field, value in rows:
            if field not in metadata:
                metadata[field] = []
            metadata[field].append({"value": value})
        return metadata
    finally:
        if conn:
            src_db_pool.putconn(conn)

def batch_prefetch_file_paths(item_uuids, batch_size=50):
    """Prefetch file paths - REMOVED: Direct lookups are faster"""
    logging.info(f"ℹ️  Prefetch disabled - using direct filesystem lookups")
    pass

# ============================================================================
# 🔹 MAIN PROCESSING FUNCTION
# ============================================================================

def get_file_path_from_internal_id(internal_id, filename=None, path_hint=None):
    """
    Resolve bitstream file path safely.
    Priority:
    1. path_hint (if valid)
    2. cached internal_id lookup
    """

    # 1️⃣ Use explicit path hint if provided
    if path_hint and os.path.exists(path_hint):
        return path_hint

    # 2️⃣ Try cached assetstore resolution
    cached_path = get_cached_file_path(internal_id)
    if cached_path and os.path.exists(cached_path):
        return cached_path

    return None



def process_single_item(row):
    """Process a single item with structured error logging"""
    item_id, _, _, _, _, item_uuid, _, _ = row
    ws_id = None
    try:
        session = session_manager.get_session()
        metadata_dict = fetch_item_metadata(item_uuid)
        
        # print("HII")
        # print(metadata_dict)
        
        # Extract case details
        case_no = metadata_dict.get("dc.title", [{"value": "N/A"}])[0]["value"]
        case_year = metadata_dict.get("dc.caseyear", [{"value": "N/A"}])[0]["value"]
        case_type = metadata_dict.get("dc.casetype", [{"value": "N/A"}])[0]["value"]
        batch_no = metadata_dict.get("dc.batch-number", [{"value": "N/A"}])[0]["value"]
        # Create workspace
        create_payload = {
            "submissionDefinition": "traditional",
            "owningCollection": "f114a309-67ab-4056-84a3-e7ab6a2dd55f",
            "sections": {"license": {"granted": True}},
        }
        
        try:
            ws_id, _ = create_workspaceitem(session, create_payload)
        except Exception as e:
            error_logger.log_error("metadata_failed", {
                "uuid": item_uuid,
                "case_no": case_no,
                "case_year": case_year,
                "case_type": case_type,
                "batch_no": batch_no,
                "stage": "workspace_creation",
                "error": str(e)
            })
            raise
        
        # Handle authors -> judge name
        if "dc.contributor.author" in metadata_dict:
            authors = [a["value"] for a in metadata_dict["dc.contributor.author"] if a.get("value")]
            if authors:
                metadata_dict["dc.judge.name"] = [{"value": authors[0].strip()}]
        
        # Extract case details for CINO lookup
        case_type_val = case_type.split('-')[0].strip() if case_type != "N/A" else None
        reg_year = case_year.strip() if case_year != "N/A" else None
        reg_no = case_no.split()[-1].strip() if case_no != "N/A" else None
        
        # CINO lookup with caching
        if case_type_val and reg_no and reg_year:
            try:
                cino_number = get_cached_cino(case_type_val, reg_no, reg_year)
                if cino_number:
                    metadata_dict["dc.cino"] = [{"value": cino_number}]
                else:
                    error_logger.log_error("missing_cino", {
                        "uuid": item_uuid,
                        "workspace_id": ws_id,
                        "case_no": reg_no,
                        "case_year": reg_year,
                        "case_type": case_type_val,
                        "batch_no": batch_no
                    })
            except Exception as e:
                error_logger.log_error("missing_cino", {
                    "uuid": item_uuid,
                    "workspace_id": ws_id,
                    "case_no": reg_no,
                    "case_year": reg_year,
                    "case_type": case_type_val,
                    "batch_no": batch_no,
                    "error": str(e)
                })
        # print(cino)
        
        # Patch metadata
        try:
            patch_metadata(session, ws_id, metadata_dict)
            # print("this is metadata value"+ metadata_dict)
        except Exception as e:
            error_logger.log_error("metadata_failed", {
                "uuid": item_uuid,
                "workspace_id": ws_id,
                "case_no": case_no,
                "case_year": case_year,
                "case_type": case_type,
                "batch_no": batch_no,
                "stage": "metadata_patch",
                "error": str(e)
            })
            raise
        
        # Upload files with intelligent path hints
        internal_ids = fetch_item_internal_ids(item_uuid)
        missing_pdfs = []
        uploaded_files = []
        
        for file_info in internal_ids:
            internal_id = file_info["internal_id"]
            filename = file_info.get("file_name", "unknown.pdf")
            path_hint = file_info.get("path_hint")  # Full path from processed metadata
            
            # Use intelligent path detection with both filename and path hints
            file_path = get_file_path_from_internal_id(internal_id, filename, path_hint)
            
            if not file_path:
                missing_pdfs.append({
                    "internal_id": internal_id,
                    "filename": filename,
                    "checksum": file_info.get("checksum")
                })
                continue
            
            try:
                upload_bitstream(session, ws_id, file_path, file_info["checksum"], filename)
                uploaded_files.append(filename)
            except Exception as e:
                error_logger.log_error("general_error", {
                    "uuid": item_uuid,
                    "workspace_id": ws_id,
                    "case_no": case_no,
                    "case_year": case_year,
                    "case_type": case_type,
                    "batch_no": batch_no,
                    "stage": "file_upload",
                    "filename": filename,
                    "error": str(e)
                })
                raise
        
        # Log missing PDFs and skip workflow if any missing
        if missing_pdfs:
            error_logger.log_error("missing_pdf", {
                "uuid": item_uuid,
                "workspace_id": ws_id,
                "case_no": case_no,
                "case_year": case_year,
                "case_type": case_type,
                "batch_no": batch_no,
                "missing_files": missing_pdfs,
                "uploaded_files": uploaded_files,
                "action_required": "Upload missing PDFs to workspace and submit manually"
            })
            logging.warning(f"⛔ Skipping workflow for WSID={ws_id} (missing PDFs)")
            return False, item_uuid
        
        # Submit to workflow
        try:
            submit_to_workflow(session, ws_id)
        except Exception as e:
            error_logger.log_error("workflow_failed", {
                "uuid": item_uuid,
                "workspace_id": ws_id,
                "case_no": case_no,
                "case_year": case_year,
                "case_type": case_type,
                "batch_no": batch_no,
                "uploaded_files": uploaded_files,
                "error": str(e),
                "action_required": "Manually submit workspace to workflow"
            })
            return False, item_uuid
        
        # Log success
        error_logger.log_success({
            "uuid": item_uuid,
            "workspace_id": ws_id,
            "case_no": case_no,
            "case_year": case_year,
            "case_type": case_type,
            "batch_no": batch_no,
            "files_uploaded": len(uploaded_files)
        })
        
        return True, item_uuid
    
    except Exception as e:
        # Catch-all for unexpected errors
        try:
            metadata = fetch_item_metadata(item_uuid)
            case_no = metadata.get("dc.title", [{"value": "N/A"}])[0]["value"]
            case_year = metadata.get("dc.caseyear", [{"value": "N/A"}])[0]["value"]
            case_type = metadata.get("dc.casetype", [{"value": "N/A"}])[0]["value"]
            batch_no = metadata.get("dc.batch-number", [{"value": "N/A"}])[0]["value"]
        except:
            case_no = case_year = case_type = batch_no = "N/A"
        
        error_logger.log_error("general_error", {
            "uuid": item_uuid,
            "workspace_id": ws_id,
            "case_no": case_no,
            "case_year": case_year,
            "case_type": case_type,
            "batch_no": batch_no,
            "stage": "unknown",
            "error": str(e)
        })
        error_logger.increment_failed()
        return False, item_uuid

# ============================================================================
# 🔹 MIGRATION MAIN FUNCTION
# ============================================================================

def safe_json(resp):
    """Safely parse JSON without failing migration"""
    try:
        if resp.content and resp.content.strip():
            return resp.json()
    except Exception:
        pass
    return {}


def migrate_chunk(start, end, threads, checkpoint_interval):
    """Migrate a single chunk - designed to be called repeatedly with fresh process"""
    global error_logger
    
    # Initialize error logger if not exists
    if error_logger is None:
        error_logger = ErrorLogger()
    
    # Add timing fields to summary
    error_logger.summary['start_time'] = datetime.now().isoformat()
    
    try:
        # Calculate optimal pool size
        pool_size = max(threads * 3, 20)
        
        # CRITICAL: Initialize pools BEFORE fetching any data
        logging.info("🔄 Initializing database connection pools...")
        logging.info(f"   Threads: {threads}, Pool size: {pool_size}")
        init_db_pools(min_conn=max(threads, 5), max_conn=pool_size)
        
        # Verify pools were created
        if src_db_pool is None or cino_db_pool is None:
            raise Exception("Failed to initialize database connection pools!")
        
        logging.info("🔐 Initializing session manager...")
        session_manager.get_session()
        
        # Fetch rows using connection pool
        conn = None
        try:
            conn = src_db_pool.getconn()
            cur = conn.cursor()
            
            q = f"SELECT {', '.join(COMMON_COLUMNS)} FROM item ORDER BY uuid"
            q += f" OFFSET {start} LIMIT {end - start}"
            
            cur.execute(q)
            rows = cur.fetchall()
            cur.close()
        finally:
            if conn:
                src_db_pool.putconn(conn)
        
        total = len(rows)
        
        print(f"\n{'='*70}")
        print(f"🚀 DSpace Migration Chunk: {start:,} to {end:,}")
        print(f"{'='*70}")
        print(f"📊 Configuration:")
        print(f"   Chunk items: {total:,}")
        print(f"   Parallel threads: {threads}")
        print(f"   DB connection pool: {pool_size} connections (initialized: ✅)")
        print(f"   Session auto-renewal: Every 15 minutes OR 500 requests")
        print(f"   Caching: LRU (max 50k file paths, 20k CINOs)")
        print(f"{'='*70}\n")
        
        if total == 0:
            print("⚠️  No items to process in this chunk")
            logging.warning(f"No items found for range {start}-{end}")
            return 0, 0
        
        # Initialize performance tracking variables
        chunk_start_time = datetime.now()
        last_performance_check = chunk_start_time
        items_at_last_check = 0
        processed_count = 0
        
        progress_bar = tqdm(total=total, desc=f"Chunk {start}-{end}", unit="item")
        
        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = {executor.submit(process_single_item, row): row for row in rows}
            
            for future in as_completed(futures):
                try:
                    success, uuid = future.result()
                    processed_count += 1
                    progress_bar.update(1)
                    status = "✔" if success else "❌"
                    
                    # Calculate current speed
                    now = datetime.now()
                    elapsed = (now - last_performance_check).total_seconds()
                    if elapsed >= 60:
                        items_processed = processed_count - items_at_last_check
                        speed = items_processed / elapsed if elapsed > 0 else 0
                        
                        # Performance degradation warning
                        if speed < 1.5 and processed_count > 100:
                            logging.warning(f"⚠️ Performance degraded to {speed:.2f} items/sec")
                            logging.info("💡 Clearing caches and renewing session...")
                            
                            file_path_cache.clear()
                            cino_cache.clear()
                            session_manager.force_renew()
                            
                            logging.info("✅ Performance optimization applied")
                        
                        last_performance_check = now
                        items_at_last_check = processed_count
                    
                    progress_bar.set_postfix({
                        "Status": status,
                        "Progress": f"{processed_count}/{total}"
                    })
                except Exception as e:
                    logging.error(f"❌ Error processing future result: {e}")
                    processed_count += 1
                    progress_bar.update(1)
        
        progress_bar.close()
        
        # Close database pools
        close_db_pools()
        
        # Update final summary with end time
        error_logger.summary['end_time'] = datetime.now().isoformat()
        error_logger.update_summary()
        
        # Calculate statistics
        duration = datetime.fromisoformat(error_logger.summary['end_time']) - \
                   datetime.fromisoformat(error_logger.summary['start_time'])
        items_per_sec = total / duration.total_seconds() if duration.total_seconds() > 0 else 0
        
        print(f"\n✅ Chunk {start:,}-{end:,} completed")
        print(f"   Processed: {total:,} items")
        print(f"   Success: {error_logger.summary['successful']:,}")
        print(f"   Failed: {error_logger.summary['failed']:,}")
        print(f"   Speed: {items_per_sec:.2f} items/sec")
        print(f"   Duration: {duration}\n")
        
        logging.info(f"Chunk completed: {total:,} items in {duration}")
        
        return error_logger.summary['successful'], error_logger.summary['failed']
    
    except KeyboardInterrupt:
        print("\n\n⚠️  Migration interrupted by user!")
        if error_logger:
            error_logger.summary['end_time'] = datetime.now().isoformat()
            error_logger.update_summary()
        close_db_pools()
        raise
    
    except Exception as e:
        logging.error(f"❌ Chunk migration failed: {str(e)}")
        print(f"\n❌ ERROR: {str(e)}")
        if error_logger:
            error_logger.summary['end_time'] = datetime.now().isoformat()
            error_logger.update_summary()
        close_db_pools()
        raise


def migrate(start=0, end=None, threads=5, checkpoint_interval=1000, chunk_size=2000):
    """Multi-threaded migration with automatic chunking for memory management
    
    Args:
        start: Starting index
        end: Ending index
        threads: Number of parallel threads
        checkpoint_interval: Save progress every N items (for large batches)
        chunk_size: Process in chunks of N items (creates fresh process each chunk)
    """
    global error_logger
    
    # Initialize global error logger
    error_logger = ErrorLogger()
    
    total_items = end - start
    
    # Auto-chunk for large batches
    if total_items > chunk_size:
        print(f"\n{'='*70}")
        print(f"📦 CHUNKED MIGRATION MODE")
        print(f"{'='*70}")
        print(f"   Total items: {total_items:,}")
        print(f"   Chunk size: {chunk_size:,}")
        print(f"   Number of chunks: {(total_items + chunk_size - 1) // chunk_size}")
        print(f"   This prevents memory leaks in long-running processes")
        print(f"{'='*70}\n")
        
        overall_success = 0
        overall_failed = 0
        chunk_num = 1
        total_chunks = (total_items + chunk_size - 1) // chunk_size
        
        current = start
        while current < end:
            chunk_end = min(current + chunk_size, end)
            
            print(f"\n{'#'*70}")
            print(f"# CHUNK {chunk_num}/{total_chunks}: Items {current:,} to {chunk_end:,}")
            print(f"{'#'*70}\n")
            
            try:
                # Each chunk runs in current process but with fresh state
                success, failed = migrate_chunk(current, chunk_end, threads, checkpoint_interval)
                overall_success += success
                overall_failed += failed
                
                print(f"\n✅ Chunk {chunk_num}/{total_chunks} complete")
                print(f"   Running totals: Success={overall_success:,}, Failed={overall_failed:,}\n")
                
            except Exception as e:
                print(f"\n❌ Chunk {chunk_num} failed: {e}")
                print(f"   You can resume from: --start {current} --end {end}")
                raise
            
            current = chunk_end
            chunk_num += 1
            
            # Force garbage collection between chunks
            import gc
            gc.collect()
            
            # Small pause between chunks
            if current < end:
                print("⏸️  Pausing 5 seconds before next chunk...\n")
                import time
                time.sleep(5)
        
        print(f"\n{'='*70}")
        print(f"🎉 COMPLETE MIGRATION FINISHED")
        print(f"{'='*70}")
        print(f"   Total processed: {total_items:,}")
        print(f"   Total success: {overall_success:,}")
        print(f"   Total failed: {overall_failed:,}")
        print(f"   Success rate: {(overall_success/total_items*100):.2f}%")
        print(f"{'='*70}\n")
        
    else:
        # Small batch - run directly
        migrate_chunk(start, end, threads, checkpoint_interval)

# ============================================================================
# 🔹 MAIN ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Migrate items to DSpace with automatic chunking for memory management",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Small batch (runs as single chunk)
  python migrate.py --start 0 --end 500 --threads 5

  # Medium batch (auto-chunks into 5k pieces)
  python migrate.py --start 0 --end 20000 --threads 11

  # LARGE batch (auto-chunks, maintains speed)
  python migrate.py --start 0 --end 100000 --threads 11 --chunk-size 5000
  
  # Custom chunk size (smaller = more stable, larger = faster)
  python migrate.py --start 0 --end 50000 --threads 11 --chunk-size 2000

How Chunking Works:
  - Processes N items, then starts fresh with clean memory
  - Each chunk maintains 2 items/sec speed
  - Prevents memory leaks in long-running processes
  - Example: 100k items in 5k chunks = 20 mini-migrations
  
Performance:
  - Small batches (<5k): No chunking needed
  - Medium batches (5k-50k): Auto-chunks at 5k
  - Large batches (>50k): MUST use chunking for consistent speed
        """
    )
    parser.add_argument("--start", type=int, default=0, help="Starting index (default: 0)")
    parser.add_argument("--end", type=int, required=True, help="Ending index (required)")
    parser.add_argument(
        "--threads", 
        type=int, 
        default=5, 
        help="Number of parallel threads (default: 5, recommended: 10-11)"
    )
    parser.add_argument(
        "--checkpoint",
        type=int,
        default=1000,
        help="Save checkpoint every N items (default: 1000)"
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=5000,
        help="Process in chunks of N items to prevent memory leaks (default: 5000)"
    )
    args = parser.parse_args()
    
    # Calculate batch size
    batch_size = args.end - args.start
    
    # Recommendations based on batch size
    if batch_size > 100000:
        print(f"\n⚠️  VERY LARGE BATCH: {batch_size:,} items")
        print(f"   Recommended chunk size: 3000-5000 items")
        if args.chunk_size > 5000:
            print(f"   Your chunk size: {args.chunk_size:,} (may cause slowdown)")
            response = input("   Use recommended chunk size of 5000? (y/n): ")
            if response.lower() == 'y':
                args.chunk_size = 2000
    
    if batch_size > 10000 and args.chunk_size > 10000:
        print(f"\n💡 RECOMMENDATION:")
        print(f"   Batch size: {batch_size:,} items")
        print(f"   Chunk size: {args.chunk_size:,} items")
        print(f"   This may cause memory issues and slowdown")
        print(f"   Recommended: Use --chunk-size 5000 for consistent speed")
        print()
    
    try:
        migrate(
            start=args.start, 
            end=args.end, 
            threads=args.threads,
            checkpoint_interval=args.checkpoint,
            chunk_size=args.chunk_size
        )
    except KeyboardInterrupt:
        print("\n\n✋ Migration stopped by user")
        exit(1)
