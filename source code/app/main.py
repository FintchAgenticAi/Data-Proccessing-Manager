from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse
from fastapi import FastAPI

from app.config import (
    AGENTIC_RULE_MANAGER_URL,
    DATABASE_PATH,
    DEDUPLICATION_SERVICE_URL,
    FILE_MANAGEMENT_URL,
    QUARANTINE_DIR,
    TRANSFORMATION_SERVICE_URL,
    VALIDATION_SERVICE_URL,
)
from app.db import connection, initialize_database
from app.file_manager_client import FileManagerClient
from app.integration_client import ServiceIntegrationClient
from app.rule_selector import DummyAgenticRuleSelector
from app.schemas import (
    PipelineRequest,
    PipelineResponse,
    ProcessingJobRequest,
    ProcessingJobResponse,
    ReuploadResponse,
    ReviewRequest,
)
from app.service import ProcessingService

app = FastAPI(
    title="Data Processing Manager API",
    version="1.0.0",
    description="Classifies File Manager CSV rows as clean or quarantined and manages quarantine review.",
)
router = APIRouter(prefix="/processing", tags=["Data Processing Manager"])
file_manager = FileManagerClient(FILE_MANAGEMENT_URL)
service = ProcessingService(DATABASE_PATH, DummyAgenticRuleSelector())
integrations = ServiceIntegrationClient(
    {
        "agentic_rule_manager": AGENTIC_RULE_MANAGER_URL,
        "transformation_service": TRANSFORMATION_SERVICE_URL,
        "validation_service": VALIDATION_SERVICE_URL,
        "deduplication_service": DEDUPLICATION_SERVICE_URL,
    }
)


@app.on_event("startup")
def startup() -> None:
    initialize_database()


@app.get("/health", include_in_schema=False)
def health() -> dict[str, str]:
    return {"status": "ok"}


@router.post("/jobs", response_model=ProcessingJobResponse, status_code=201, summary="Process a File Manager CSV")
async def process_csv(request: ProcessingJobRequest) -> ProcessingJobResponse:
    details = await file_manager.get_details(request.csv_filename)
    csv_filename = details.get("filename")
    if not isinstance(csv_filename, str) or not csv_filename:
        raise HTTPException(status_code=502, detail="File Manager returned incomplete CSV details.")
    file_manager_batch_id = details.get("data_batch_id")
    if request.data_batch_id is not None and file_manager_batch_id not in (None, request.data_batch_id):
        raise HTTPException(
            status_code=409,
            detail="File Manager filename and data_batch_id do not match.",
        )
    if csv_filename != request.csv_filename:
        raise HTTPException(
            status_code=409,
            detail="File Manager returned a different CSV filename than requested.",
        )
    with tempfile.TemporaryDirectory(prefix="file-manager-csv-") as temp_dir:
        csv_path = await file_manager.download_csv(csv_filename, Path(temp_dir))
        return service.process(
            csv_path,
            details,
            request.supplier_name,
            request.country,
            data_batch_id=request.data_batch_id or file_manager_batch_id,
        )


@router.post(
    "/uploads",
    response_model=ProcessingJobResponse,
    status_code=201,
    summary="Receive a converted CSV from File Manager",
    description="Stores upload details and CSV rows directly in the Data Processing Manager database.",
)
async def receive_upload(
    file: UploadFile = File(...),
    supplier_name: str = Form(..., min_length=1),
    country: str = Form(..., min_length=1),
    batch_name: str = Form(..., min_length=1),
    year: int = Form(..., ge=1900, le=2100),
    file_type: str = Form(..., min_length=1),
    data_category: str = Form(..., min_length=1),
    data_batch_id: int | None = Form(default=None, ge=1),
) -> ProcessingJobResponse:
    if not file.filename or Path(file.filename).suffix.lower() != ".csv":
        raise HTTPException(status_code=422, detail="Data Processing Manager accepts CSV files only.")
    with tempfile.TemporaryDirectory(prefix="file-manager-upload-") as temp_dir:
        csv_path = Path(temp_dir) / Path(file.filename).name
        csv_path.write_bytes(await file.read())
        details = {
            "filename": Path(file.filename).name,
            "original_filename": Path(file.filename).name,
            "batch_name": batch_name,
            "year": year,
            "file_type": file_type,
            "data_category": data_category,
            "country": country,
        }
        return service.process(
            csv_path,
            details,
            supplier_name,
            country,
            data_batch_id=data_batch_id,
        )


@router.post(
    "/batches/{data_batch_id}/pipeline",
    response_model=PipelineResponse,
    status_code=201,
    summary="Run the external transformation, validation, and deduplication pipeline",
    description=(
        "Calls the Agentic Rule Manager before each stage, then calls the Transformation, "
        "Validation, and Deduplication services. Configure the service URLs with environment variables."
    ),
)
async def run_pipeline(data_batch_id: int, request: PipelineRequest) -> PipelineResponse:
    required_services = (
        "agentic_rule_manager",
        "transformation_service",
        "validation_service",
        "deduplication_service",
    )
    missing = [name for name in required_services if not integrations.configured(name)]
    if missing:
        raise HTTPException(
            status_code=503,
            detail=f"Pipeline services are not configured: {', '.join(missing)}.",
        )
    return await service.run_pipeline(
        data_batch_id,
        integrations,
        {
            "rule_selector_path": request.rule_selector_path,
            "transformation_path": request.transformation_path,
            "validation_path": request.validation_path,
            "deduplication_path": request.deduplication_path,
        },
    )


@router.get("/batches/{data_batch_id}/quarantine-export", response_class=FileResponse, summary="Download quarantine CSV")
def download_quarantine_export(data_batch_id: int) -> FileResponse:
    with connection(DATABASE_PATH) as db:
        row = db.execute(
            "SELECT qe.file_name FROM quarantine_export qe JOIN error_reason er ON er.error_id=qe.error_id JOIN quarantine_data q ON q.quarantine_id=er.quarantine_id JOIN raw_kegal_data r ON r.raw_record_id=q.raw_record_id WHERE r.data_batch_id=? ORDER BY qe.export_id DESC LIMIT 1",
            (data_batch_id,),
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="No quarantine export exists for this batch.")
    path = QUARANTINE_DIR / row["file_name"]
    if not path.is_file():
        raise HTTPException(status_code=404, detail="Quarantine export file is missing.")
    return FileResponse(path, media_type="text/csv", filename=path.name)


@router.post("/quarantine/{quarantine_id}/review", summary="Confirm or release a quarantined row")
def review_quarantine(quarantine_id: int, request: ReviewRequest) -> dict[str, str]:
    with connection(DATABASE_PATH) as db:
        row = db.execute("SELECT raw_record_id FROM quarantine_data WHERE quarantine_id=?", (quarantine_id,)).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Quarantine record not found.")
        status = "released" if request.decision.value == "release" else "confirmed"
        db.execute("UPDATE quarantine_data SET status=?, reason=COALESCE(?, reason) WHERE quarantine_id=?", (status, request.reason, quarantine_id))
        if status == "released":
            db.execute("INSERT OR IGNORE INTO clean_data(raw_record_id) VALUES (?)", (row["raw_record_id"],))
    return {"status": status, "quarantine_id": str(quarantine_id)}


@router.post("/batches/{data_batch_id}/reupload", response_model=ReuploadResponse, summary="Re-upload reviewed CSV as a new version")
async def reupload_csv(
    data_batch_id: int,
    version: int = Form(..., ge=1),
    file: UploadFile = File(...),
) -> ReuploadResponse:
    if not file.filename or Path(file.filename).suffix.lower() != ".csv":
        raise HTTPException(status_code=422, detail="Only CSV files can be re-uploaded.")
    with connection(DATABASE_PATH) as db:
        batch = db.execute("SELECT * FROM data_batch WHERE data_batch_id=?", (data_batch_id,)).fetchone()
        if batch is None:
            raise HTTPException(status_code=404, detail="Data batch not found.")
        new_filename = f"{batch['year']}_{batch['batch_name']}_v{version}.csv"
        target = QUARANTINE_DIR / new_filename
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("wb") as output:
            shutil.copyfileobj(file.file, output)
        supplier = db.execute("SELECT supplier_name FROM supplier WHERE supplier_id=?", (batch["supplier_id"],)).fetchone()
    details = {"filename": new_filename, "batch_name": batch["batch_name"], "year": batch["year"], "csv_path": str(target)}
    result = service.process(
        target,
        details,
        supplier["supplier_name"],
        batch["country"],
        data_batch_id=data_batch_id,
        version=version,
    )
    return ReuploadResponse(message="CSV re-uploaded as a new version.", new_filename=new_filename, version=version, data_batch_id=result.data_batch_id)


app.include_router(router)
