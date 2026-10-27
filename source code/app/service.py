from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from app.config import QUARANTINE_DIR
from app.db import connection
from app.integration_client import ServiceIntegrationClient
from app.rule_selector import AgenticRuleSelector
from app.schemas import ProcessingJobResponse, ProcessingStatus
from app.schemas import PipelineResponse


class ProcessingService:
    def __init__(self, database_path: Path, rule_selector: AgenticRuleSelector):
        self.database_path = database_path
        self.rule_selector = rule_selector

    async def run_pipeline(
        self,
        data_batch_id: int,
        client: ServiceIntegrationClient,
        paths: dict[str, str],
    ) -> PipelineResponse:
        with connection(self.database_path) as db:
            batch = db.execute(
                "SELECT b.*, s.supplier_name FROM data_batch b JOIN supplier s ON s.supplier_id=b.supplier_id WHERE b.data_batch_id=?",
                (data_batch_id,),
            ).fetchone()
            if batch is None:
                raise HTTPException(status_code=404, detail="Data batch not found.")
            raw_rows = db.execute(
                "SELECT raw_record_id, `row_number`, data_json FROM raw_kegal_data WHERE data_batch_id=? ORDER BY `row_number`",
                (data_batch_id,),
            ).fetchall()
            execution = db.execute(
                "INSERT INTO pipeline_execution(data_batch_id,status) VALUES (?, 'processing') RETURNING pipeline_execution_id",
                (data_batch_id,),
            ).fetchone()
            pipeline_id = int(execution["pipeline_execution_id"])
            rows = [
                {"raw_record_id": row["raw_record_id"], "row_number": row["row_number"], **json.loads(row["data_json"])}
                for row in raw_rows
            ]
            context = {
                "data_batch_id": data_batch_id,
                "csv_filename": batch["csv_filename"],
                "supplier_name": batch["supplier_name"],
                "batch_name": batch["batch_name"],
                "country": batch["country"],
                "year": batch["year"],
            }

        try:
            rules_transform = await self._pipeline_call(
                client, pipeline_id, "agentic_rule_manager", "transformation_rule_selection",
                paths["rule_selector_path"], {"stage": "transformation", "context": context, "rows": rows},
            )
            transformed = await self._pipeline_call(
                client, pipeline_id, "transformation_service", "transformation",
                paths["transformation_path"], {"context": context, "rules": rules_transform.get("rules", rules_transform), "rows": rows},
            )
            transformed_rows = transformed.get("rows", rows)
            rules_validation = await self._pipeline_call(
                client, pipeline_id, "agentic_rule_manager", "validation_rule_selection",
                paths["rule_selector_path"], {"stage": "validation", "context": context, "rows": transformed_rows},
            )
            validation = await self._pipeline_call(
                client, pipeline_id, "validation_service", "validation",
                paths["validation_path"], {"context": context, "rules": rules_validation.get("rules", rules_validation), "rows": transformed_rows},
            )
            rules_deduplication = await self._pipeline_call(
                client, pipeline_id, "agentic_rule_manager", "deduplication_rule_selection",
                paths["rule_selector_path"], {"stage": "deduplication", "context": context, "rows": transformed_rows},
            )
            deduplication = await self._pipeline_call(
                client, pipeline_id, "deduplication_service", "deduplication",
                paths["deduplication_path"], {"context": context, "rules": rules_deduplication.get("rules", rules_deduplication), "rows": transformed_rows},
            )
            return self._complete_pipeline(
                pipeline_id, data_batch_id, raw_rows, transformed_rows, validation, deduplication
            )
        except Exception as exc:
            with connection(self.database_path) as db:
                db.execute(
                    "UPDATE pipeline_execution SET status='failed', error_message=?, completed_at=CURRENT_TIMESTAMP WHERE pipeline_execution_id=?",
                    (str(exc), pipeline_id),
                )
            raise

    async def _pipeline_call(
        self,
        client: ServiceIntegrationClient,
        pipeline_id: int,
        service_name: str,
        stage_name: str,
        endpoint: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        response = await client.call(service_name, endpoint, payload)
        with connection(self.database_path) as db:
            db.execute(
                "INSERT INTO service_call(pipeline_execution_id,service_name,stage_name,request_json,response_json,status) VALUES (?,?,?,?,?,'completed')",
                (pipeline_id, service_name, stage_name, json.dumps(payload), json.dumps(response)),
            )
        return response

    def _complete_pipeline(
        self,
        pipeline_id: int,
        data_batch_id: int,
        raw_rows: list[Any],
        transformed_rows: list[dict[str, Any]],
        validation: dict[str, Any],
        deduplication: dict[str, Any],
    ) -> PipelineResponse:
        validation_results = validation.get("results", [])
        duplicate_results = deduplication.get("results", [])
        invalid_ids = {
            int(item.get("raw_record_id", item.get("row_number", -1)))
            for item in validation_results
            if item.get("valid") is False
        }
        duplicate_ids = {
            int(item.get("raw_record_id", item.get("row_number", -1)))
            for item in duplicate_results
            if item.get("duplicate") is True
        }
        quarantine_ids = invalid_ids | duplicate_ids
        clean_count = quarantine_count = 0
        with connection(self.database_path) as db:
            for index, raw in enumerate(raw_rows):
                raw_id = int(raw["raw_record_id"])
                transformed = transformed_rows[index] if index < len(transformed_rows) else json.loads(raw["data_json"])
                status = "quarantine" if raw_id in quarantine_ids or int(raw["row_number"]) in quarantine_ids else "clean"
                db.execute(
                    "UPDATE raw_kegal_data SET processing_status=?, transformed_data_json=? WHERE raw_record_id=?",
                    (status, json.dumps(transformed), raw_id),
                )
                if status == "quarantine":
                    quarantine_count += 1
                    db.execute(
                        "INSERT INTO quarantine_data(raw_record_id,status,reason) VALUES (?, 'quarantined', ?)",
                        (raw_id, "External validation or deduplication service flagged this row."),
                    )
                else:
                    clean_count += 1
                    db.execute("INSERT OR IGNORE INTO clean_data(raw_record_id) VALUES (?)", (raw_id,))
            db.execute(
                "UPDATE pipeline_execution SET status='completed', completed_at=CURRENT_TIMESTAMP WHERE pipeline_execution_id=?",
                (pipeline_id,),
            )
        return PipelineResponse(
            pipeline_execution_id=pipeline_id,
            data_batch_id=data_batch_id,
            status="completed",
            processed_count=len(raw_rows),
            clean_count=clean_count,
            quarantine_count=quarantine_count,
        )

    def process(
        self,
        csv_path: Path,
        file_details: dict[str, Any],
        supplier_name: str,
        country: str,
        data_batch_id: int | None = None,
        version: int | None = None,
    ) -> ProcessingJobResponse:
        if not csv_path.is_file():
            raise HTTPException(status_code=404, detail="CSV file is not available for processing.")
        batch_name = file_details["batch_name"]
        year = int(file_details["year"])
        csv_filename = file_details["filename"]
        with connection(self.database_path) as db:
            supplier = db.execute(
                "INSERT INTO supplier(supplier_name) VALUES (?) ON CONFLICT(supplier_name) DO UPDATE SET supplier_name=excluded.supplier_name RETURNING supplier_id",
                (supplier_name,),
            ).fetchone()
            supplier_id = int(supplier["supplier_id"])
            batch = db.execute(
                "SELECT * FROM data_batch WHERE data_batch_id=?",
                (data_batch_id,),
            ).fetchone() if data_batch_id is not None else None
            if batch is None and data_batch_id is None:
                batch = db.execute(
                    "SELECT * FROM data_batch WHERE csv_filename=? ORDER BY data_batch_id LIMIT 1",
                    (csv_filename,),
                ).fetchone()
                if batch is not None:
                    data_batch_id = int(batch["data_batch_id"])
            if batch is not None and batch["csv_filename"] != csv_filename and version is None:
                batch = db.execute(
                    "SELECT * FROM data_batch WHERE csv_filename=? ORDER BY data_batch_id LIMIT 1",
                    (csv_filename,),
                ).fetchone()
                if batch is None:
                    data_batch_id = None
            if batch is None:
                batch_version = version if version is not None else self._next_version(db, supplier_id, batch_name, country, year)
                if data_batch_id is None:
                    batch = db.execute(
                        """
                        INSERT INTO data_batch(supplier_id,batch_name,country,year,version,csv_filename)
                        VALUES (?,?,?,?,?,?)
                        RETURNING data_batch_id
                        """,
                        (supplier_id, batch_name, country, year, batch_version, csv_filename),
                    ).fetchone()
                else:
                    batch = db.execute(
                        """
                        INSERT INTO data_batch(data_batch_id,supplier_id,batch_name,country,year,version,csv_filename)
                        VALUES (?,?,?,?,?,?,?)
                        RETURNING data_batch_id
                        """,
                        (data_batch_id, supplier_id, batch_name, country, year, batch_version, csv_filename),
                    ).fetchone()
            elif version is not None:
                db.execute(
                    "UPDATE data_batch SET version=?, csv_filename=? WHERE data_batch_id=?",
                    (version, csv_filename, int(batch["data_batch_id"])),
                )
            batch_id = int(batch["data_batch_id"])
            execution = db.execute(
                "INSERT INTO execution_results(data_batch_id,result_status,processing_status) VALUES (?, 'pending', ?) RETURNING execution_result_id",
                (batch_id, ProcessingStatus.processing.value),
            ).fetchone()
            execution_id = int(execution["execution_result_id"])
            clean_count = quarantine_count = count = 0
            quarantine_rows: list[tuple[int, dict[str, Any], str]] = []
            with csv_path.open("r", encoding="utf-8-sig", newline="") as csv_file:
                for row_number, row in enumerate(csv.DictReader(csv_file), start=2):
                    count += 1
                    raw = db.execute(
                        "INSERT INTO raw_kegal_data(data_batch_id,`row_number`,data_json) VALUES (?,?,?) RETURNING raw_record_id",
                        (batch_id, row_number, json.dumps(row)),
                    ).fetchone()
                    raw_id = int(raw["raw_record_id"])
                    decision = self.rule_selector.select(row, file_details)
                    if decision.is_quarantine:
                        quarantine_count += 1
                        quarantine_rows.append((raw_id, row, decision.reason or "Rule selector requested quarantine"))
                        quarantine = db.execute(
                            "INSERT INTO quarantine_data(raw_record_id,status,reason) VALUES (?, 'quarantined', ?) RETURNING quarantine_id",
                            (raw_id, decision.reason or "Rule selector requested quarantine"),
                        ).fetchone()
                        db.execute(
                            "INSERT INTO error_reason(quarantine_id,error_reason) VALUES (?,?)",
                            (int(quarantine["quarantine_id"]), decision.reason or "Rule selector requested quarantine"),
                        )
                    else:
                        clean_count += 1
                        db.execute("INSERT INTO clean_data(raw_record_id) VALUES (?)", (raw_id,))
            if quarantine_rows:
                self._export(db, quarantine_rows, batch_id, batch_name, year)
            db.execute(
                "UPDATE execution_results SET result_status='completed', processing_status='completed', processing_count=?, clean_count=?, quarantine_count=?, completed_at=CURRENT_TIMESTAMP WHERE execution_result_id=?",
                (count, clean_count, quarantine_count, execution_id),
            )
            processed_attempt = db.execute(
                "SELECT COUNT(*) AS processed_attempt FROM execution_results WHERE data_batch_id=?",
                (batch_id,),
            ).fetchone()["processed_attempt"]
        return ProcessingJobResponse(
            data_batch_id=batch_id, csv_filename=csv_filename,
            processed_count=count,
            processed_attempt=int(processed_attempt),
            clean_count=clean_count,
            quarantine_count=quarantine_count,
        )

    @staticmethod
    def _next_version(db, supplier_id: int, batch_name: str, country: str, year: int) -> int:
        row = db.execute(
            "SELECT COALESCE(MAX(version), 0) + 1 AS next_version FROM data_batch WHERE supplier_id=? AND batch_name=? AND country=? AND year=?",
            (supplier_id, batch_name, country, year),
        ).fetchone()
        return int(row["next_version"])

    @staticmethod
    def _export(db, rows: list[tuple[int, dict[str, Any], str]], batch_id: int, batch_name: str, year: int) -> str:
        QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
        export_name = f"{year}_{batch_name}_quarantine.csv"
        export_path = QUARANTINE_DIR / export_name
        fields = sorted({key for _, row, _ in rows for key in row} | {"quarantine_reason", "raw_record_id"})
        with export_path.open("w", encoding="utf-8", newline="") as output:
            writer = csv.DictWriter(output, fieldnames=fields)
            writer.writeheader()
            for raw_id, row, reason in rows:
                values = dict(row)
                values.update(raw_record_id=raw_id, quarantine_reason=reason)
                writer.writerow(values)
        for raw_id, _, _ in rows:
            quarantine = db.execute("SELECT quarantine_id FROM quarantine_data WHERE raw_record_id=?", (raw_id,)).fetchone()
            error = db.execute("SELECT error_id FROM error_reason WHERE quarantine_id=?", (quarantine["quarantine_id"],)).fetchone()
            db.execute("INSERT INTO quarantine_export(error_id,file_name,status) VALUES (?,?,'created')", (error["error_id"], export_name))
        return f"/processing/batches/{batch_id}/quarantine-export"
