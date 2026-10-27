# Data Processing Manager

This service processes CSV files created by File Manager. It requests CSV metadata from
File Manager, classifies each row through an `AgenticRuleSelector`, stores the result in
SQLite, and exports quarantined rows for user review.

## Run locally

Start File Manager first:

```powershell
Set-Location "C:\Users\USER\Desktop\project\file_management"
.\.venv\Scripts\python.exe -m uvicorn app.main:app --app-dir "Source code" --reload --port 8000
```

Start Data Processing Manager in another terminal:

```powershell
Set-Location "C:\Users\USER\Desktop\project\data_processing_manager"
..\file_management\.venv\Scripts\python.exe -m uvicorn app.main:app --app-dir "source code" --reload --port 8001
```

Swagger is available at `http://127.0.0.1:8001/docs`.

## Workflow

1. Upload a file to File Manager using `POST /upload`.
2. Start processing with `POST /processing/jobs`:

   ```json
   {
     "csv_filename": "2026_batch-a.csv",
     "supplier_name": "Supplier A",
     "country": "US"
   }
   ```

   The Data Processing Manager requests the CSV metadata from
   `GET /files/{filename}/details`, then downloads the converted CSV through
   `GET /files/{filename}/content`. The services therefore do not need to share
   the File Manager's local filesystem.

3. Download quarantined rows from
   `GET /processing/batches/{data_batch_id}/quarantine-export`.
4. Review each row with
   `POST /processing/quarantine/{quarantine_id}/review`, using `release` when the
   row is valid or `keep_quarantined` when it is not.
5. Re-upload a corrected CSV with
   `POST /processing/batches/{data_batch_id}/reupload`. The service increments only
   the version; batch name and year remain unchanged.

File Manager now exposes `GET /files/{filename}/details` and
`GET /files/{filename}/content` for downstream services.

## Agentic selector integration

Replace `DummyAgenticRuleSelector` in
[source code/app/main.py](./source%20code/app/main.py) with the other developer's
implementation of the `AgenticRuleSelector` protocol in
[source code/app/rule_selector.py](./source%20code/app/rule_selector.py).
The selector receives one row and the File Manager metadata and returns a
`RuleDecision`.
