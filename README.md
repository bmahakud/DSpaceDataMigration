# DSpace Migration Script — Developer Documentation

> **Purpose:** Migrate court case documents (PDFs + metadata) from a legacy DSpace PostgreSQL database into a new DSpace 7 instance via REST API.  
> **Audience:** Developers onboarding to this project with no prior context.

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Architecture & Data Flow](#2-architecture--data-flow)
3. [Prerequisites & Setup](#3-prerequisites--setup)
4. [Configuration Reference](#4-configuration-reference)
5. [Authentication Flow](#5-authentication-flow)
6. [Core Migration Pipeline](#6-core-migration-pipeline)
7. [Database Layer](#7-database-layer)
8. [File Resolution (Assetstore)](#8-file-resolution-assetstore)
9. [Caching Strategy](#9-caching-strategy)
10. [Error Handling & Log Files](#10-error-handling--log-files)
11. [Running the Script](#11-running-the-script)
12. [Chunked Execution for Large Batches](#12-chunked-execution-for-large-batches)
13. [Troubleshooting Common Issues](#13-troubleshooting-common-issues)
14. [Key Classes & Functions Reference](#14-key-classes--functions-reference)

---

## 1. System Overview

```
┌─────────────────────┐         ┌──────────────────────┐         ┌─────────────────────┐
│   Source DSpace DB  │         │    Migration Script  │         │  Target DSpace 7     │
│  (PostgreSQL/batch) │──READ──▶│    migrate.py        │──POST──▶│  REST API :8080      │
└─────────────────────┘         └──────────────────────┘         └─────────────────────┘
                                          │
                                          │──READ──▶ CINO DB (cisnc @ 10.184.240.25)
                                          │
                                          │──READ──▶ Assetstore (mounted NFS paths)
```

The script reads items from the **source DSpace database**, enriches them with a **CINO number** (a case identifier from a separate court database), resolves the **physical PDF files** from NFS-mounted assetstores, and pushes everything to the **target DSpace 7 REST API**.

---

## 2. Architecture & Data Flow

Each item goes through this exact pipeline:

```
[Fetch Item Row from DB]
         │
         ▼
[Fetch Item Metadata]  ──▶  dc.title, dc.casetype, dc.caseyear, dc.batch-number ...
         │
         ▼
[Create WorkspaceItem]  ──▶  POST /api/submission/workspaceitems  →  returns workspace_id
         │
         ▼
[Enrich Metadata]
   ├── Map dc.contributor.author  →  dc.judge.name
   └── Lookup CINO from cisnc DB  →  dc.cino
         │
         ▼
[PATCH Metadata]  ──▶  PATCH /api/submission/workspaceitems/{workspace_id}
         │
         ▼
[Resolve & Upload PDFs]  ──▶  POST /api/submission/workspaceitems/{workspace_id}
         │                        (multipart file upload, one per PDF)(If any one pdf is missing from multiple the entire item will fail)
         ▼
[Submit to Workflow]  ──▶  POST /api/workflow/workflowitems
         │
         ▼
[Log Success or Error]
```

---

## 3. Prerequisites & Setup

### Python Dependencies

```bash
pip install requests psycopg2-binary tqdm
```

### Required Access

| Resource | Details |
|---|---|
| Source DB | `localhost:5432` / dbname `batch` / user `anthem` |
| CINO DB | `10.184.240.25:5432` / dbname `cisnc` / user `postgres` |
| DSpace API | `http://10.184.240.87:8080/server/api` |
| Assetstore | NFS mounts at `/mnt/Digitaization1`, `/mnt/external`, `/mnt/filing`, `/mnt/rrdc`, `/mnt/internal` |

Make sure all NFS mounts are active before running:

```bash
df -h | grep mnt
```

---

## 4. Configuration Reference

All config lives at the top of `migrate.py`:

```python
BASE_URL = "http://10.184.240.87:8080/server/api"   # DSpace REST endpoint
USERNAME = "admin@gmail.com"
PASSWORD = "admin"

SRC_DB   = { ... }   # Source DSpace PostgreSQL
CINO_DB  = { ... }   # Court case CINO lookup DB
```

### Assetstore Path Map

The script searches multiple NFS-mounted directories to find the physical PDF file. The `ASSETSTORE_PREFIX_MAP` maps filename prefixes to specific mount points — this is a performance hint to avoid exhaustive directory scanning:

```python
ASSETSTORE_PREFIX_MAP = {
    "hcserver":      "/mnt/filing/assetstore",
    "assetstore2":   "/mnt/digitaization1/assetstore",
    "hcserverasset2":"/mnt/rrdc/assetstore",
    "external":      "/mnt/external/assetstore",
    "internal":      "/mnt/internal",
}
```

If a prefix doesn't match, the script falls back to scanning all paths in `ASSETSTORE_PATHS`.

---

## 5. Authentication Flow

The DSpace 7 REST API uses a **CSRF token + JWT** two-step authentication. This is handled automatically by `SessionManager`.

### Step 1 — Obtain CSRF Token

```
GET  /api/authn/status
     └──▶ Response sets cookie: DSPACE-XSRF-COOKIE (or XSRF-TOKEN)
```

The CSRF token must be extracted from the response cookies and sent as `X-XSRF-TOKEN` on all subsequent requests.

### Step 2 — Login and get JWT

```
POST /api/authn/login
     Headers: X-XSRF-TOKEN: <csrf_token>
     Body:    user=admin@gmail.com&password=admin
     └──▶ Response Header: Authorization: Bearer <jwt_token>
```

The JWT is stored in the session and attached to all future requests as `Authorization: Bearer ...`.

### Session Expiry & Auto-Renewal

Sessions expire every 20 minutes. The `SessionManager` class handles this transparently:

```python
session_manager = SessionManager(session_lifetime_minutes=15)
```

- **Every API call** goes through `session_manager.get_session()`.
- If the session is older than 15 minutes, it is **silently renewed** before returning.
- If a `401 Unauthorized` error is received mid-flight, `@retry_on_auth_failure` decorator forces an immediate re-auth and retries the call once.

> **Key point for new devs:** You never call `requests.Session()` directly. Always use `session_manager.get_session()`. This ensures you always have a valid, fresh session without thinking about it.

---

## 6. Core Migration Pipeline

### Step 1 — Create WorkspaceItem

A workspace item is a **draft submission** in DSpace. Every item must start here.

```python
ws_id, _ = create_workspaceitem(session, {
    "submissionDefinition": "traditional",
    "owningCollection": "f114a309-67ab-4056-84a3-e7ab6a2dd55f",
    "sections": {"license": {"granted": True}}
})
```

- **Endpoint:** `POST /api/submission/workspaceitems`
- **Returns:** `workspace_id` (integer) — used in all subsequent calls for this item.
- If this fails, the item is logged to `metadata_failed.log` and skipped.

### Step 2 — Patch Metadata

Metadata is applied to the workspace item via a **JSON Patch** (RFC 6902) request.

```python
patch_metadata(session, ws_id, metadata_dict)
```

- **Endpoint:** `PATCH /api/submission/workspaceitems/{ws_id}`
- **Content-Type:** `application/json-patch+json`
- The patch body is an array of `{"op": "add", "path": "...", "value": ...}` operations.
- Only fields defined in `allowed_fields` inside `patch_metadata()` are sent — unknown fields are silently dropped.
- Two extra operations are always appended:
  - License acceptance: `{"op": "add", "path": "/sections/license/granted", "value": true}`
  - Collection assignment: hardcoded collection UUID

**Metadata field mapping (source DB → DSpace field):**

| Source DB Field | DSpace Metadata Field | Notes |
|---|---|---|
| `dc.title` | `dc.title` | Case number |
| `dc.casetype` | `dc.casetype` | e.g. `CRL-001` |
| `dc.caseyear` | `dc.caseyear` | Registration year |
| `dc.contributor.author` | `dc.judge.name` | First author mapped to judge |
| *(CINO DB lookup)* | `dc.cino` | Court case identifier |
| `dc.batch-number` | `dc.batch-number` | Batch tracking |

### Step 3 — CINO Lookup

Before patching metadata, the script enriches the item with a **CINO number** — a unique court case identifier stored in a separate database (`cisnc`).

```python
cino_number = get_cached_cino(case_type_val, reg_no, reg_year)
```

The lookup queries the `civil_t_a` table joined with `case_type_t`:

```sql
SELECT c.cino FROM civil_t_a c
JOIN case_type_t ct ON c.filcase_type = ct.case_type
WHERE ct.type_name = %s AND c.reg_no = %s AND c.reg_year = %s
LIMIT 1;
```

If no CINO is found, the item is logged to `missing_cino.log` but **processing continues** — a missing CINO is non-fatal.

### Step 4 — Upload PDFs

Each item can have **multiple PDFs** (bistreams in DSpace terminology). They are uploaded one by one as multipart form data.

```python
upload_bitstream(session, ws_id, file_path, checksum, filename)
```

- **Endpoint:** `POST /api/submission/workspaceitems/{ws_id}` (same as create, but with file)
- The file is sent with `Content-Type: application/pdf`.
- Upload timeout scales with file size: `max(60, file_size_mb * 2)` seconds.
- If **any PDF is missing** from the assetstore, the item is logged to `missing_pdf.log` and workflow submission is **skipped** — the item stays as a draft workspace item for manual intervention.

### Step 5 — Submit to Workflow

Once all PDFs are uploaded, the workspace item is submitted for review/approval.

```python
submit_to_workflow(session, ws_id)
```

- **Endpoint:** `POST /api/workflow/workflowitems`
- **Content-Type:** `text/uri-list`
- **Body:** The full URI of the workspace item: `http://.../api/submission/workspaceitems/{ws_id}`
- On failure, logged to `workflow_failed.log`. The PDFs and metadata are already uploaded — the workspace item exists and can be manually submitted.

---

## 7. Database Layer

### Connection Pooling

The script uses `psycopg2.pool.ThreadedConnectionPool` to manage DB connections safely across threads.

```python
init_db_pools(min_conn=5, max_conn=30)
```

Pool sizing rule: `max_conn = max(threads * 3, 20)` — each thread may need up to 3 simultaneous connections (source DB + CINO DB + metadata fetch).

Always use the pool, never create raw connections:

```python
conn = src_db_pool.getconn()
try:
    # use conn
finally:
    src_db_pool.putconn(conn)   # MUST return connection to pool
```

### Key Queries

**Fetch item rows for migration:**
```sql
SELECT item_id, in_archive, withdrawn, last_modified,
       discoverable, uuid, submitter_id, owning_collection
FROM item
ORDER BY uuid
OFFSET {start} LIMIT {chunk_size}
```

**Fetch bitstream (PDF) internal IDs for an item:**
```sql
SELECT b.internal_id, b.checksum, title_mv.text_value AS file_name
FROM item i
JOIN item2bundle i2b ON i2b.item_id = i.uuid
JOIN bundle2bitstream b2b ON b2b.bundle_id = i2b.bundle_id
JOIN bitstream b ON b.uuid = b2b.bitstream_id
LEFT JOIN metadatavalue title_mv ON ...
WHERE i.uuid = %s AND title_mv.text_value ILIKE '%.pdf'
```

---

## 8. File Resolution (Assetstore)

DSpace stores files on disk using a **hierarchical directory structure** derived from the file's `internal_id`. Given an `internal_id` like `a1b2c3d4e5f6...`, the path is:

```
{assetstore_base}/a1/b2/c3/a1b2c3d4e5f6...
```

The script implements this in `get_cached_file_path()`:

```python
subdirs = "/".join([internal_id[i:i+2] for i in range(0, 6, 2)])
# "a1b2c3..." → "a1/b2/c3"

for base in ASSETSTORE_PATHS:
    full_path = f"{base}/{subdirs}/{internal_id}"
    if os.path.exists(full_path):
        return full_path
```

It searches across all 5 mounted assetstore paths until it finds the file.

---

## 9. Caching Strategy

Two LRU caches prevent redundant DB queries and filesystem scans:

| Cache | Key | Max Size | Purpose |
|---|---|---|---|
| `file_path_cache` | `internal_id` | 50,000 entries | Avoid re-scanning assetstore paths |
| `cino_cache` | `case_type\|reg_no\|reg_year` | 20,000 entries | Avoid repeated CINO DB lookups |

Both are instances of the custom `LRUCache` class which is thread-safe and evicts the oldest entries when full.

If performance degrades below 1.5 items/sec, the script **automatically clears both caches** and renews the session:

```python
if speed < 1.5 and processed_count > 100:
    file_path_cache.clear()
    cino_cache.clear()
    session_manager.force_renew()
```

---

## 10. Error Handling & Log Files

All errors are written to separate log files under `migration_logs/`:

| Log File | When Written |
|---|---|
| `success.log` | Item fully processed and submitted to workflow |
| `missing_pdf.log` | One or more PDFs not found on any assetstore path |
| `missing_cino.log` | CINO number not found in cisnc DB (non-fatal) |
| `workflow_failed.log` | Workspace + PDFs OK, but workflow submission failed |
| `metadata_failed.log` | Workspace creation or metadata patch failed |
| `auth_failed.log` | Authentication/session errors |
| `general_error.log` | Unexpected errors not matching above categories |
| `summary.log` | Final success/failure counts per run |

Each log line format:
```
{uuid} | WSID={workspace_id} | Case={case_type} {case_no}/{case_year} | Batch={batch_no} | {error_message}
```

> **Recovery tip:** Items in `workflow_failed.log` are the easiest to recover — their data is already in DSpace, they just need manual workflow submission. Items in `missing_pdf.log` need the PDF to be located and uploaded to the workspace manually before submission.

---

## 11. Running the Script

```bash
# Basic usage
python migrate.py --start 0 --end 1000 --threads 5

# Recommended for production (11 threads, chunked)
python migrate.py --start 0 --end 50000 --threads 11 --chunk-size 5000

# Resume from a specific offset after failure
python migrate.py --start 12000 --end 50000 --threads 11
```

### Arguments

| Argument | Default | Description |
|---|---|---|
| `--start` | `0` | Row offset to start from (inclusive) |
| `--end` | *(required)* | Row offset to stop at (exclusive) |
| `--threads` | `5` | Parallel worker threads |
| `--checkpoint` | `1000` | Log progress every N items |
| `--chunk-size` | `5000` | Items per memory-managed chunk |

---

## 12. Chunked Execution for Large Batches

For batches larger than `chunk_size`, the script automatically splits work into chunks:

```
100,000 items ÷ 5,000 per chunk = 20 chunks
```

Each chunk:
1. Initializes fresh DB connection pools
2. Fetches only its slice of rows
3. Processes with thread pool
4. Closes pools and forces garbage collection
5. Pauses 5 seconds before the next chunk

This prevents memory leaks and keeps throughput consistent (~2 items/sec) across very long runs.

---

## 13. Troubleshooting Common Issues

**`No CSRF token found`**  
The DSpace API didn't return a cookie on `/authn/status`. Check if the server is up and the `BASE_URL` is correct.

**`Login failed: 401`**  
Wrong credentials. Check `USERNAME` / `PASSWORD` in config.

**`Database pool initialization failed`**  
Check DB connectivity. Verify the host, port, and credentials for both `SRC_DB` and `CINO_DB`. Also confirm the user has `SELECT` permissions.

**`File not found on any assetstore path`**  
The NFS mount is missing or the file was never digitized. Check `df -h` for mount status. Items will appear in `missing_pdf.log`.

**Speed drops below 1.5 items/sec**  
The script auto-recovers by clearing caches and renewing the session. If it persists, check DB query times and network latency to the DSpace API.

**`Patch failed: 422 Unprocessable Entity`**  
A metadata field was rejected by DSpace validation. Check `allowed_fields` in `patch_metadata()` and compare against the DSpace submission form configuration.

---

## 14. Key Classes & Functions Reference

| Class / Function | Description |
|---|---|
| `SessionManager` | Manages DSpace API session lifecycle, auto-renews on expiry or 401 |
| `ErrorLogger` | Writes structured logs to separate files per error type |
| `LRUCache` | Thread-safe LRU cache with configurable max size |
| `init_db_pools()` | Creates psycopg2 connection pools for src + cino DBs |
| `get_cached_cino()` | CINO lookup with LRU cache fallback to DB |
| `get_cached_file_path()` | Resolve `internal_id` → filesystem path with LRU cache |
| `create_workspaceitem()` | POST to create a draft submission in DSpace |
| `patch_metadata()` | PATCH metadata fields onto a workspace item |
| `upload_bitstream()` | Upload a PDF file to a workspace item |
| `submit_to_workflow()` | Move workspace item into the DSpace review workflow |
| `process_single_item()` | Orchestrates the full pipeline for one item |
| `migrate_chunk()` | Processes one chunk with thread pool + progress bar |
| `migrate()` | Entry point — auto-chunks large batches and calls `migrate_chunk()` |

