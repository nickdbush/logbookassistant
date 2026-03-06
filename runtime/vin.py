"""Identifier → technical type resolution via Logbook API."""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
import requests

LOGBOOK_API_URL = os.environ.get("LOGBOOK_API_URL", "https://api.joinlogbook.com")


def _db_path() -> Path:
    return Path(os.environ.get("DATA_DIR", "/app/data")) / "metadata.duckdb"


def resolve_identifier(identifier: str, db_path: Path | None = None) -> dict:
    """Resolve a VIN or VRM to a technical type via the Logbook API.

    Returns dict with keys: tt_id, tt_code, brand_name, series_name,
    model_name, tt_name, series_icecode, sn_min, sn_max, vin, vrm, source.

    Raises ValueError if identifier cannot be resolved.
    """
    if db_path is None:
        db_path = _db_path()

    api_key = os.environ.get("SERVICE_API_KEY")
    if not api_key:
        raise ValueError("SERVICE_API_KEY environment variable is required")

    resp = requests.post(
        f"{LOGBOOK_API_URL}/app/machines/resolve",
        json={"identifier": identifier.strip()},
        headers={"Authorization": f"Bearer {api_key}"},
        timeout=10,
    )
    if resp.status_code != 200:
        raise ValueError(f"Logbook API error ({resp.status_code}): {resp.text}")

    data = resp.json()
    specs = data.get("specs")
    if not specs:
        raise ValueError(f"No profile resolved for identifier: {identifier}")

    tt_id = int(specs["technicalTypeId"])
    db_info = _lookup_tt_info(tt_id, db_path)

    return {
        "tt_id": tt_id,
        "tt_code": specs.get("technicalTypeCode"),
        "brand_name": specs.get("brand"),
        "series_name": specs.get("series"),
        "model_name": specs.get("model"),
        "tt_name": db_info["tt_name"] if db_info else None,
        "series_icecode": db_info["series_icecode"] if db_info else None,
        "sn_min": specs.get("serialMin"),
        "sn_max": specs.get("serialMax"),
        "vin": data.get("vin"),
        "vrm": data.get("vrm"),
        "source": "logbook_api",
    }


def _lookup_tt_info(tt_id: int, db_path: Path) -> dict | None:
    """Look up technical type info from DuckDB."""
    db = duckdb.connect(str(db_path), read_only=True)
    try:
        row = db.execute(
            "SELECT tt_name, series_icecode FROM technical_types WHERE tt_id = ?",
            [tt_id],
        ).fetchone()
        if row:
            return {"tt_name": row[0], "series_icecode": row[1]}
    finally:
        db.close()
    return None
