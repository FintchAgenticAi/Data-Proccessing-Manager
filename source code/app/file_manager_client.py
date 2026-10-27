from __future__ import annotations

from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx
from fastapi import HTTPException


class FileManagerClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")

    async def get_details(self, filename: str) -> dict[str, Any]:
        try:
            async with httpx.AsyncClient(base_url=self.base_url, timeout=15) as client:
                response = await client.get(f"/files/{quote(filename, safe='')}/details")
        except httpx.HTTPError as exc:
            raise HTTPException(status_code=502, detail="File Manager is unavailable.") from exc
        if response.status_code != 200:
            raise HTTPException(status_code=404, detail="CSV details were not found in File Manager.")
        return response.json()

    async def download_csv(self, filename: str, target_dir: Path) -> Path:
        """Download a File Manager CSV into a local path for processing."""
        safe_filename = Path(filename).name
        if not safe_filename or safe_filename != filename or Path(safe_filename).suffix.lower() != ".csv":
            raise HTTPException(status_code=422, detail="A valid CSV filename is required.")

        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / safe_filename
        try:
            async with httpx.AsyncClient(base_url=self.base_url, timeout=30) as client:
                response = await client.get(f"/files/{quote(filename, safe='')}/content")
        except httpx.HTTPError as exc:
            raise HTTPException(status_code=502, detail="File Manager is unavailable.") from exc
        if response.status_code != 200:
            raise HTTPException(status_code=404, detail="CSV content was not found in File Manager.")
        target_path.write_bytes(response.content)
        return target_path
