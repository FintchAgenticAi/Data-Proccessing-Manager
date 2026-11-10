# Data Processing Manager – Workflow & API Guide

FastAPI service that takes CSVs produced by **File Manager**, classifies every row as
**clean** or **quarantined**, stores results in a database, and manages the quarantine
review / re-upload cycle. It can also orchestrate external Transformation, Validation and
Deduplication services guided by an Agentic Rule Manager.

Swagger UI: `http://127.0.0.1:8002/docs` (Docker, port 8002) or `:8001` / `:8000` depending on how you start uvicorn (see [README.md](./README.md)).
OpenAPI JSON: `/openapi.json` (a snapshot is in [openapi.json](./openapi.json)).

## 1. High-level flow

```
 User ──upload──> File Manager ──(CSV + metadata)──> Data Processing Manager (DPM)
                                                        │
                        ┌───────────────────────────────┤
                        │ 1. POST /processing/jobs      │  classify rows (RuleSelector)
                        │ 2. (optional) POST .../pipeline  → Agentic Rule Manager
                        │                                   → Transformation → Validation → Deduplication
                        ▼
              ┌──────── clean_data         (rows OK)
              └──────── quarantine_data    (rows flagged) ──> quarantine CSV export
                                                │
              3. GET  .../quarantine-export  ◄──┘  user downloads & fixes
              4. POST /quarantine/{id}/review      release → moves row to clean_data
              5. POST .../reupload                 corrected CSV → new version (v2, v3…)
```

Step by step:

1. **Upload** the file to File Manager (`POST /upload` there).
2. **Process** – `POST /processing/jobs` with the CSV filename. DPM calls File Manager
   `GET /files/{filename}/details` (metadata: batch_name, year, data_batch_id) and
   `GET /files/{filename}/content` (download CSV to a temp dir). Each row goes through the
   rule selector; results are saved as raw → clean / quarantine rows. Quarantined rows are
   written to a CSV in `source code/data/quarantine/`.
3. **Pipeline (optional)** – `POST /processing/batches/{id}/pipeline` runs the external
   services in order, each preceded by a rule-selection call to the Agentic Rule Manager.
4. **Download** quarantined rows, fix them offline.
5. **Review** each row: `release` (valid → inserted into `clean_data`) or `keep_quarantined`.
6. **Re-upload** the corrected CSV as a new version; it is re-processed under the same batch.

## 2. Components

| File (in `source code/app/`) | Role |
|---|---|
| [main.py](./source%20code/app/main.py) | FastAPI app, router `/processing`, all endpoints, startup DB init, `/health` |
| [service.py](./source%20code/app/service.py) | `ProcessingService`: `process()` (row classification, DB writes, quarantine export), `run_pipeline()` (external service orchestration), version handling |
| [rule_selector.py](./source%20code/app/rule_selector.py) | `AgenticRuleSelector` protocol + `DummyAgenticRuleSelector` (empty row, bad `email`, status markers such as `invalid/fail/yes`). Swap in a real implementation in `main.py` |
| [file_manager_client.py](./source%20code/app/file_manager_client.py) | httpx client for File Manager: `get_details`, `download_csv` (validates `.csv` filename, blocks path traversal) |
| [integration_client.py](./source%20code/app/integration_client.py) | httpx client for the four external services (`agentic_rule_manager`, `transformation_service`, `validation_service`, `deduplication_service`); 503 if URL unset, 502 if unreachable |
| [schemas.py](./source%20code/app/schemas.py) | Pydantic request/response models and enums |
| [db.py](./source%20code/app/db.py) | DB connection (MySQL default, SQLite optional) and table creation |
| [config.py](./source%20code/app/config.py) | Environment-based settings |

### Database tables
`supplier`, `data_batch`, `raw_kegal_data` (every CSV row as JSON + status), `clean_data`,
`quarantine_data` (status `PENDING_FIX` / `FIXED`), `error_reason`, `quarantine_export`,
`execution_results`, `rule_selected`, `rule_for_column`, `pipeline_execution`,
`service_call` (request/response log of each external call).

### Configuration (environment variables)

| Variable | Default | Purpose |
|---|---|---|
| `FILE_MANAGEMENT_URL` | `http://127.0.0.1:8001` | File Manager base URL |
| `DATABASE_BACKEND` | `mysql` | set `sqlite` for local file DB |
| `DATABASE_URL` | built from MySQL vars | full SQLAlchemy URL override |
| `MYSQL_HOST/PORT/DATABASE/USER/PASSWORD` | `127.0.0.1` / `3307` / `data_processing_db` / `dulari` / empty | MySQL connection |
| `DATABASE_PATH` | `source code/data/data_processing_manager.sqlite3` | SQLite file |
| `AGENTIC_RULE_MANAGER_URL`, `TRANSFORMATION_SERVICE_URL`, `VALIDATION_SERVICE_URL`, `DEDUPLICATION_SERVICE_URL` | empty | required only for the pipeline endpoint |

Put secrets (e.g. `MYSQL_PASSWORD`) in `.env` (see `.env.example`); never commit them.

### Run
```powershell
cd "source code"
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8002
# or: docker compose up --build
```

## 3. API reference (Swagger tag: *Data Processing Manager*)

All endpoints are under `/processing`. `GET /health` (hidden from Swagger) returns `{"status":"ok"}`.

### 3.1 `POST /processing/jobs` – Process a File Manager CSV  (201)
Request:
```json
{ "csv_filename": "2026_batch-a.csv", "supplier_name": "Supplier A", "country": "US", "data_batch_id": null }
```
Response (`ProcessingJobResponse`):
```json
{ "data_batch_id": 1, "csv_filename": "2026_batch-a.csv", "processed_count": 100,
  "processed_attempt": 1, "clean_count": 95, "quarantine_count": 5 }
```
Errors: `404` file not found in File Manager · `409` filename/batch id mismatch ·
`502` File Manager unavailable or incomplete details.

### 3.2 `POST /processing/batches/{data_batch_id}/pipeline` – Run external pipeline  (201)
Request (all optional, shown with defaults):
```json
{ "rule_selector_path": "/rules/select", "transformation_path": "/transform",
  "validation_path": "/validate", "deduplication_path": "/deduplicate" }
```
Sequence: rule manager (transformation) → Transformation → rule manager (validation) →
Validation → rule manager (deduplication) → Deduplication. Rows with `valid:false` or
`duplicate:true` (matched by `raw_record_id`/`row_number`) go to quarantine; others to clean.
Every call is logged in `service_call`; failures mark `pipeline_execution` as `failed`.

Response (`PipelineResponse`):
```json
{ "pipeline_execution_id": 1, "data_batch_id": 1, "status": "completed",
  "processed_count": 100, "clean_count": 90, "quarantine_count": 10 }
```
Errors: `404` batch not found · `503` a service URL is not configured · `502` service unreachable/error.

### 3.3 `GET /processing/batches/{data_batch_id}/quarantine-export` – Download quarantine CSV
Returns the latest quarantine CSV (`text/csv`) for the batch.
Errors: `404` no export for the batch or file missing on disk.

### 3.4 `POST /processing/quarantine/{quarantine_id}/review` – Confirm or release a row
Request:
```json
{ "decision": "release", "reason": "Verified manually" }
```
`decision`: `release` (row → `FIXED`, copied to `clean_data`) or `keep_quarantined` (stays `PENDING_FIX`).
Response: `{ "status": "released", "quarantine_id": "12" }` (`status` is `released` or `confirmed`).
Error: `404` quarantine record not found.

### 3.5 `POST /processing/batches/{data_batch_id}/reupload` – Re-upload corrected CSV as new version  (multipart)
Form fields: `version` (int ≥ 1), `file` (`.csv`).
The file is saved as `{year}_{batch_name}_v{version}.csv` in the quarantine folder and
re-processed for the same batch.
Response (`ReuploadResponse`):
```json
{ "message": "CSV re-uploaded as a new version.", "new_filename": "2026_batch-a_v2.csv",
  "version": 2, "data_batch_id": 1 }
```
Errors: `422` not a CSV · `404` batch not found.

## 4. Swagger walkthrough
1. Open `/docs`, expand **Data Processing Manager**.
2. `POST /processing/jobs` → *Try it out* → execute with the example body; note `data_batch_id`.
3. `GET /processing/batches/{id}/quarantine-export` → download.
4. `POST /processing/quarantine/{id}/review` → release valid rows.
5. `POST /processing/batches/{id}/reupload` → upload fixed CSV with the next version number.
6. (Optional) `POST /processing/batches/{id}/pipeline` once external service URLs are set.

## 5. Extending
Implement `AgenticRuleSelector.select(row, file_details) -> RuleDecision` and replace
`DummyAgenticRuleSelector()` where `ProcessingService` is created in `main.py`.
