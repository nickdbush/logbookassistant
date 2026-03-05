"""VIN → technical type resolution.

Primary: S3 shard lookup (SHA-256 of last 8 VIN chars).
Fallback: CNH Store API → tt_code → local DuckDB lookup.
"""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

import duckdb
import requests

ROOT = Path(__file__).resolve().parent.parent.parent
DUCKDB_PATH = ROOT / "data" / "metadata.duckdb"

S3_BUCKET = "spectinga-warrantai"
S3_VIN_PREFIX = "vin/2026-01-23"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
)
BRANDS = ["newhollandag", "caseih"]


def resolve_vin(vin: str, db_path: Path = DUCKDB_PATH) -> dict:
    """Resolve a VIN to a technical type.

    Returns dict with keys: tt_id, tt_code, brand_name, series_name,
    model_name, tt_name, series_icecode, source.

    Raises ValueError if VIN cannot be resolved.
    """
    vin = vin.strip().upper()

    # Try S3 shard lookup first
    tt_id = _shard_lookup(vin)
    if tt_id is not None:
        info = _lookup_tt_info(tt_id, db_path)
        if info:
            info["source"] = "shard"
            return info

    # Fallback: CNH Store API
    tt_code = _cnh_store_lookup(vin)
    if tt_code:
        info = _tt_code_to_info(tt_code, vin, db_path)
        if info:
            info["source"] = "cnh_api"
            return info

    raise ValueError(f"Could not resolve VIN: {vin}")


def _shard_lookup(vin: str) -> int | None:
    """Look up VIN in S3 shard files."""
    try:
        import boto3
    except ImportError:
        return None

    if len(vin) < 8:
        return None

    last8 = vin[-8:]
    sha = hashlib.sha256(last8.encode()).hexdigest()
    shard_key = sha[:2]

    try:
        s3 = boto3.client("s3")
        key = f"{S3_VIN_PREFIX}/sha256_{shard_key}.tsv"
        resp = s3.get_object(Bucket=S3_BUCKET, Key=key)
        body = resp["Body"].read().decode("utf-8")

        for line in body.split("\n"):
            parts = line.strip().split("\t")
            if len(parts) >= 3 and parts[0] == vin:
                try:
                    return int(parts[2])
                except ValueError:
                    pass
    except Exception:
        pass

    return None


def _lookup_tt_info(tt_id: int, db_path: Path) -> dict | None:
    """Look up technical type info from DuckDB."""
    db = duckdb.connect(str(db_path), read_only=True)
    try:
        row = db.execute(
            "SELECT tt_id, tt_code, brand_name, series_name, model_name, "
            "tt_name, series_icecode FROM technical_types WHERE tt_id = ?",
            [tt_id],
        ).fetchone()
        if row:
            return dict(zip(
                ["tt_id", "tt_code", "brand_name", "series_name",
                 "model_name", "tt_name", "series_icecode"],
                row,
            ))
    finally:
        db.close()
    return None


def _cnh_store_lookup(vin: str) -> str | None:
    """Call CNH Store API to resolve VIN → tt_code."""
    for brand in BRANDS:
        try:
            model_info = _get_model_info(brand, vin)
            if not model_info:
                continue
            model_code = model_info["model"]
            sn_info = _get_serial_number_info(brand, model_code, vin)
            if sn_info and sn_info.get("technicalType"):
                # technicalType format: "HACT7210*ND101457 - CVT"
                tt_str = sn_info["technicalType"]
                tt_code = tt_str.split(" - ")[0].strip()
                return tt_code
        except Exception:
            continue
    return None


def _get_model_info(brand: str, vin: str) -> dict | None:
    """Query CNH Store autocomplete API."""
    url = f"https://www.mycnhstore.com/gb/en/{brand}/search/autocomplete/ManualSearchHomePageComponent"
    headers = {
        "Referer": f"https://www.mycnhstore.com/gb/en/{brand}?site=gb&clear=true",
        "User-Agent": USER_AGENT,
        "X-Requested-With": "XMLHttpRequest",
    }
    resp = requests.get(url, params={"term": vin, "byModel": "2"}, headers=headers)
    if resp.status_code != 200:
        return None
    data = resp.json()
    sns = data.get("serialNumbers", [])
    if len(sns) == 1:
        return sns[0]
    return None


def _get_serial_number_info(brand: str, model: str, vin: str) -> dict | None:
    """Query CNH Store serial number info API and parse HTML response."""
    url = f"https://www.mycnhstore.com/gb/en/{brand}/cn/serialNumberInfo/{model}"
    headers = {
        "Referer": f"https://www.mycnhstore.com/gb/en/{brand}?site=gb&clear=true",
        "User-Agent": USER_AGENT,
        "X-Requested-With": "XMLHttpRequest",
    }
    resp = requests.get(
        url, params={"model": model, "serialNumber": vin}, headers=headers
    )
    if resp.status_code != 200:
        return None
    data = resp.json()
    html = data.get("serialNumberWindowRows", "")
    return _parse_serial_number_html(html)


def _parse_serial_number_html(html: str) -> dict:
    """Parse serial number info HTML without external HTML parser.

    Extracts key-value pairs from the serial number window rows.
    """
    result = {
        "engineSerialNumber": None,
        "productionDate": None,
        "serialRange": None,
        "technicalType": None,
        "variants": [],
    }

    # Extract key-value pairs from the HTML
    # Pattern: look for element text pairs
    key_pattern = re.compile(
        r'serial-number__body__element__text[^>]*>([^<]+)<', re.IGNORECASE
    )
    # Find all sn-element blocks
    blocks = re.split(r'sn-element', html)
    for block in blocks[1:]:  # skip first empty split
        keys = key_pattern.findall(block)
        # Find the sn-row value
        val_match = re.search(r'sn-row[^>]*>([^<]+)<', block)
        if not keys or not val_match:
            continue
        key = keys[0].strip()
        value = val_match.group(1).strip()

        if key == "Engine Serial Number":
            result["engineSerialNumber"] = value
        elif key == "Production Date":
            result["productionDate"] = value
        elif key == "Serial Range":
            result["serialRange"] = value
        elif key == "Technical Type":
            result["technicalType"] = value
        elif key == "Variante":
            result["variants"].append(value)

    return result


def _tt_code_to_info(tt_code: str, vin: str, db_path: Path) -> dict | None:
    """Look up tt_code in DuckDB, disambiguate with VIN range matching."""
    db = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = db.execute(
            "SELECT tt_id, tt_code, brand_name, series_name, model_name, "
            "tt_name, series_icecode FROM technical_types WHERE tt_code = ?",
            [tt_code],
        ).fetchall()
    finally:
        db.close()

    if not rows:
        return None

    cols = ["tt_id", "tt_code", "brand_name", "series_name",
            "model_name", "tt_name", "series_icecode"]

    if len(rows) == 1:
        return dict(zip(cols, rows[0]))

    # Multiple matches — need serial range disambiguation
    # Look up TechnicalType serial ranges from DuckDB
    # For now, just return the first match
    # TODO: full serial range disambiguation requires TechnicalType data in DuckDB
    return dict(zip(cols, rows[0]))


def vin_matches_pattern(vin: str, pattern: str) -> bool:
    """Check if VIN matches a wildcard pattern (from tt_code or serial range).

    '*' in pattern matches any character at that position.
    """
    if len(vin) != len(pattern):
        return False
    for v, p in zip(vin, pattern):
        if p == "*":
            continue
        if v != p:
            return False
    return True


def vin_in_range(vin: str, sn_min: str | None, sn_max: str | None) -> bool:
    """Check if VIN falls within a serial number range (wildcard-aware).

    Uses sn_min's '*' positions as the mask. At non-wildcard positions,
    checks vin >= sn_min and vin <= sn_max lexicographically.
    NULL/empty min means indeterminate → True. NULL/empty max → open-ended.
    """
    if not sn_min or sn_min.upper() == "NULL":
        return True

    # Clean trailing ' -' from min values (seen in TechnicalType data)
    sn_min = sn_min.rstrip().rstrip("-").rstrip()

    if len(vin) != len(sn_min):
        return True  # indeterminate

    # Build wildcard mask from sn_min
    mask = [c != "*" for c in sn_min]

    masked_vin = "".join(vin[i] for i in range(len(vin)) if mask[i])
    masked_min = "".join(sn_min[i] for i in range(len(sn_min)) if mask[i])

    if masked_vin < masked_min:
        return False

    if not sn_max or sn_max.upper() == "NULL" or sn_max == "":
        return True  # open-ended

    if len(vin) != len(sn_max):
        return True

    masked_max = "".join(sn_max[i] for i in range(len(sn_max)) if i < len(mask) and mask[i])
    return masked_vin <= masked_max
