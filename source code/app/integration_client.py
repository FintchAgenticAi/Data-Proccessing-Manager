from __future__ import annotations

from typing import Any

import httpx
from fastapi import HTTPException


class ServiceIntegrationClient:
    def __init__(self, urls: dict[str, str]):
        self.urls = {name: url.rstrip("/") for name, url in urls.items() if url}

    def configured(self, service_name: str) -> bool:
        return bool(self.urls.get(service_name))

    async def call(self, service_name: str, endpoint: str, payload: dict[str, Any]) -> dict[str, Any]:
        base_url = self.urls.get(service_name)
        if not base_url:
            raise HTTPException(
                status_code=503,
                detail=f"{service_name} URL is not configured.",
            )
        try:
            async with httpx.AsyncClient(base_url=base_url, timeout=60) as client:
                response = await client.post(endpoint, json=payload)
        except httpx.HTTPError as exc:
            raise HTTPException(status_code=502, detail=f"{service_name} is unavailable.") from exc
        if response.status_code >= 400:
            raise HTTPException(
                status_code=502,
                detail=f"{service_name} rejected the pipeline request: {response.text}",
            )
        try:
            result = response.json()
        except ValueError as exc:
            raise HTTPException(status_code=502, detail=f"{service_name} returned invalid JSON.") from exc
        if not isinstance(result, dict):
            raise HTTPException(status_code=502, detail=f"{service_name} must return a JSON object.")
        return result
