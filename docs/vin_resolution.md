# Identifier Resolution & Technical Type Lookup

## Concepts

A **VIN** (Vehicle Identification Number) is the 17-character alphanumeric identifier for a specific machine. A **VRM** (Vehicle Registration Mark) is a UK licence plate number. The goal of identifier resolution is to map either to a **Technical Type (TT)**, which determines what repair operations, documentation, and specs apply to that machine.

Key identifiers:

| Identifier | Example | Description |
|------------|---------|-------------|
| VIN | `HACT7210VPD100757` | Unique machine identifier (17 chars, no I/O/Q) |
| VRM | `SN19BNF` | UK vehicle registration mark |
| tt_code | `696195807` | Technical type code |
| tt_id | `34680` | Numeric ID for a technical type — used as folder name in output |

## Resolution via Logbook API

All VIN/VRM resolution is handled by the Logbook API, which encapsulates profile lookup, VRM-to-VIN conversion, and spec extraction in a single call.

### Endpoint

```
POST https://api.joinlogbook.com/app/machines/resolve
Authorization: Bearer $SERVICE_API_KEY
Content-Type: application/json

{"identifier": "SN19BNF"}
```

### Response

```json
{
  "id": "mch_6yha3cwUTmqp6ViAi3KVk",
  "vrm": "SN19BNF",
  "vin": "HACT7260LKD401789",
  "profile": "profiles/cnh/2026-01-23/23800",
  "specs": {
    "brand": "NEW HOLLAND",
    "series": "T7. Auto Command - TIER 4B FINAL and STAGE IV  STAGE V",
    "model": "T7.260 AutoCommand",
    "variant": "Sidewinder II - STAGE V",
    "technicalTypeId": "34680",
    "technicalTypeCode": "696195807",
    "serialMin": "HACT7260*ND401323",
    "serialMax": null
  }
}
```

The `specs.technicalTypeId` maps to `tt_id` (int) in the local DuckDB `technical_types` table.

### Local enrichment

After resolving via the API, we look up the `tt_id` in DuckDB to get two fields not in the API response:
- `tt_name` — human-readable technical type name
- `series_icecode` — the ICE code for the series (e.g., `A.A.01.034`)

The `series_icecode` links the resolved machine back to the IU corpus (which is organised by series).

## Environment variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SERVICE_API_KEY` | Yes | — | Bearer token for the Logbook API |
| `LOGBOOK_API_URL` | No | `https://api.joinlogbook.com` | API base URL |

## Series folders and the product hierarchy

The source data at `/Volumes/logbookdata/cnh/iso/repository/AGCE/data/series` contains ~1,479 series folders. Each folder name is a product hierarchy ICE code: `Brand.Type.Product.Series`.

Example: `A.A.01.034` = CASE IH > Tractors > Agricultural > PUMA CVX

Each series folder contains `docs.zip` (IU documentation — what this pipeline processes) and `scripts.zip` (relational data — product info, VINs, repair times, IU-to-document mappings).

## Full resolution chain

```
VIN or VRM
    ↓
Logbook API (/app/machines/resolve)
    ↓
specs.technicalTypeId → tt_id (int)
    ↓
DuckDB technical_types → tt_name, series_icecode
    ↓
series_icecode links to IU corpus (canonical_ius.appearances)
```
