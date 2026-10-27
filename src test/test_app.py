from pathlib import Path
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from app.main import app
from app.rule_selector import DummyAgenticRuleSelector
from app.service import ProcessingService


def test_health() -> None:
    assert TestClient(app).get("/health").json() == {"status": "ok"}


def test_openapi_documents_processing_endpoints() -> None:
    document = TestClient(app).get("/openapi.json").json()
    assert "/processing/jobs" in document["paths"]
    assert "/processing/quarantine/{quarantine_id}/review" in document["paths"]
    assert "/processing/batches/{data_batch_id}/reupload" in document["paths"]
    assert "/processing/batches/{data_batch_id}/pipeline" in document["paths"]


def test_pipeline_requires_external_service_configuration(monkeypatch) -> None:
    monkeypatch.setattr("app.main.integrations.configured", lambda name: False)

    response = TestClient(app).post(
        "/processing/batches/1/pipeline",
        json={},
    )

    assert response.status_code == 503
    assert "not configured" in response.json()["detail"]


def test_processing_classifies_rows_and_writes_quarantine_export(tmp_path: Path, monkeypatch) -> None:
    csv_path = tmp_path / "input.csv"
    csv_path.write_text("name,email\nAda,ada@example.com\nBad,bad-email\n", encoding="utf-8")
    service = ProcessingService(tmp_path / "manager.sqlite3", DummyAgenticRuleSelector())
    monkeypatch.setattr("app.service.QUARANTINE_DIR", tmp_path / "quarantine")

    result = service.process(
        csv_path,
        {"filename": "input.csv", "batch_name": "batch-a", "year": 2026},
        "Supplier A",
        "US",
        data_batch_id=42,
    )

    assert result.processed_attempt == 1
    assert result.processed_count == 2
    assert result.clean_count == 1
    assert result.quarantine_count == 1
    export = tmp_path / "quarantine" / "2026_batch-a_quarantine.csv"
    assert export.exists()
    assert "bad-email" in export.read_text(encoding="utf-8")

    second_result = service.process(
        csv_path,
        {"filename": "input.csv", "batch_name": "batch-a", "year": 2026},
        "Supplier A",
        "US",
        data_batch_id=42,
    )
    assert second_result.processed_attempt == 2


def test_temporary_validation_marker_increases_quarantine_count(tmp_path: Path, monkeypatch) -> None:
    csv_path = tmp_path / "marked.csv"
    csv_path.write_text(
        "name,email,validation_status\n"
        "Ada,ada@example.com,valid\n"
        "Bad,bad@example.com,invalid\n",
        encoding="utf-8",
    )
    service = ProcessingService(tmp_path / "manager.sqlite3", DummyAgenticRuleSelector())
    monkeypatch.setattr("app.service.QUARANTINE_DIR", tmp_path / "quarantine")

    result = service.process(
        csv_path,
        {"filename": "marked.csv", "batch_name": "batch-marked", "year": 2026},
        "Supplier A",
        "US",
        data_batch_id=43,
    )

    assert result.processed_count == 2
    assert result.clean_count == 1
    assert result.quarantine_count == 1


def test_processing_allocates_new_id_when_requested_id_belongs_to_another_file(tmp_path: Path) -> None:
    first_csv = tmp_path / "first.csv"
    second_csv = tmp_path / "second.csv"
    first_csv.write_text("name,email\nFirst,first@example.com\n", encoding="utf-8")
    second_csv.write_text("name,email\nSecond,second@example.com\n", encoding="utf-8")
    service = ProcessingService(tmp_path / "manager.sqlite3", DummyAgenticRuleSelector())

    first = service.process(
        first_csv,
        {"filename": "first.csv", "batch_name": "first", "year": 2026},
        "Supplier A",
        "US",
        data_batch_id=1,
    )
    second = service.process(
        second_csv,
        {"filename": "second.csv", "batch_name": "second", "year": 2026},
        "Supplier A",
        "US",
        data_batch_id=1,
    )

    assert first.data_batch_id == 1
    assert second.data_batch_id == 2


def test_processing_job_downloads_csv_from_file_manager(tmp_path: Path, monkeypatch) -> None:
    csv_path = tmp_path / "downloaded.csv"
    csv_path.write_text("name,email\nAda,ada@example.com\n", encoding="utf-8")
    details = {
        "filename": "2026_batch-a.csv",
        "data_batch_id": 1,
        "batch_name": "batch-a",
        "year": 2026,
        "csv_path": "C:\\file-management\\Output\\2026_batch-a.csv",
    }
    monkeypatch.setattr("app.main.file_manager.get_details", AsyncMock(return_value=details))
    monkeypatch.setattr(
        "app.main.file_manager.download_csv",
        AsyncMock(side_effect=lambda filename, target_dir: _copy_fixture(csv_path, target_dir / filename)),
    )
    monkeypatch.setattr(
        "app.main.service.process",
        lambda path, metadata, supplier, country, **kwargs: _processing_result(metadata),
    )

    response = TestClient(app).post(
        "/processing/jobs",
        json={
            "csv_filename": "2026_batch-a.csv",
            "data_batch_id": 1,
            "supplier_name": "Supplier A",
            "country": "US",
        },
    )

    assert response.status_code == 201
    assert response.json()["csv_filename"] == "2026_batch-a.csv"
    assert response.json()["data_batch_id"] == 1


def test_processing_job_generates_batch_id_when_file_manager_does_not_provide_one(
    tmp_path: Path, monkeypatch
) -> None:
    csv_path = tmp_path / "downloaded.csv"
    csv_path.write_text("name,email\nAda,ada@example.com\n", encoding="utf-8")
    details = {
        "filename": "2026_batch-a.csv",
        "data_batch_id": None,
        "batch_name": "batch-a",
        "year": 2026,
        "csv_path": "C:\\file-management\\Output\\2026_batch-a.csv",
    }
    monkeypatch.setattr("app.main.file_manager.get_details", AsyncMock(return_value=details))
    monkeypatch.setattr(
        "app.main.file_manager.download_csv",
        AsyncMock(side_effect=lambda filename, target_dir: _copy_fixture(csv_path, target_dir / filename)),
    )
    monkeypatch.setattr(
        "app.main.service.process",
        lambda path, metadata, supplier, country, **kwargs: _processing_result(
            {**metadata, "data_batch_id": 42}
        ),
    )

    response = TestClient(app).post(
        "/processing/jobs",
        json={
            "csv_filename": "2026_batch-a.csv",
            "supplier_name": "Supplier A",
            "country": "US",
        },
    )

    assert response.status_code == 201
    assert response.json()["data_batch_id"] == 42


def test_processing_rejects_file_manager_identity_mismatch(monkeypatch) -> None:
    monkeypatch.setattr(
        "app.main.file_manager.get_details",
        AsyncMock(
            return_value={
                "filename": "2026_batch-a.csv",
                "data_batch_id": 2,
                "batch_name": "batch-a",
                "year": 2026,
            }
        ),
    )

    response = TestClient(app).post(
        "/processing/jobs",
        json={
            "csv_filename": "2026_batch-a.csv",
            "data_batch_id": 1,
            "supplier_name": "Supplier A",
            "country": "US",
        },
    )

    assert response.status_code == 409
    assert "do not match" in response.json()["detail"]


def _copy_fixture(source: Path, target: Path) -> Path:
    target.write_bytes(source.read_bytes())
    return target


def _processing_result(details: dict[str, object]):
    from app.schemas import ProcessingJobResponse, ProcessingStatus

    data_batch_id = details.get("data_batch_id")
    return ProcessingJobResponse(
        data_batch_id=int(data_batch_id) if data_batch_id is not None else 42,
        csv_filename=str(details["filename"]),
        processed_count=1,
        processed_attempt=1,
        clean_count=1,
        quarantine_count=0,
    )
