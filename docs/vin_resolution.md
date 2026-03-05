# VIN Resolution & Technical Type Lookup

## Concepts

A **VIN** (Vehicle Identification Number) is the 17-character alphanumeric identifier for a specific machine. The goal of VIN resolution is to map a VIN to a **Technical Type (TT)**, which determines what repair operations, documentation, and specs apply to that machine.

Key identifiers:

| Identifier | Example | Description |
|------------|---------|-------------|
| VIN | `HACT7210VPD100757` | Unique machine identifier (17 chars, no I/O/Q) |
| tt_code | `HACT7210*ND101457` | Technical type code — a pattern with wildcards |
| tt_id | `42` | Numeric ID for a technical type — used as folder name in output |
| srt_code | `SRT001` | Links a TT to its repair time table |
| column_number | `3` | Column within the repair time table for this TT |

Different models and variants have different `tt_code`s — this is the primary discriminator. Within the same `tt_code`, multiple `tt_id`s can exist, distinguished by serial number ranges (e.g., different production runs). A single `tt_id` maps to exactly one `srt_code` + `column_number` pair, which determines its repair operations.

## Series folders and the product hierarchy

The source data at `/Volumes/logbookdata/cnh/iso/repository/AGCE/data/series` contains ~1,479 series folders. Each folder name is a product hierarchy ICE code: `Brand.Type.Product.Series`.

Example: `A.A.01.034` = CASE IH > Tractors > Agricultural > PUMA CVX

Each series folder contains `docs.zip` (IU documentation — what this pipeline processes) and `scripts.zip` (relational data — product info, VINs, repair times, IU-to-document mappings).

The `ViewProducts.perfsql` table inside `scripts.zip` maps the series to its models and technical types. A single series can contain multiple models (e.g., PUMA 150 CVX, PUMA 165 CVX) and multiple technical types per model (distinguished by serial number ranges).

```
Series folder (A.A.01.034)
  └── scripts.zip
        └── ViewProducts.perfsql
              └── rows: brand, series, model, tt_id, tt_code, serial ranges
```

This is the link between the IU corpus built by this pipeline and the TT-based profiles on S3:
- **This pipeline** extracts IUs from `docs.zip` across all series, deduplicates, and tracks which series each IU appears in (via `canonical_ius.appearances`)
- **The arbortext Rust pipeline** reads `scripts.zip` from the same series folders to build VIN shards, TT profiles, and documentation indexes keyed by `tt_id`
- **At runtime**, a VIN resolves to a `tt_id`, which selects a profile and documentation index on S3. The documentation index references IU miuids — the same IDs used in this pipeline's corpus

The series folder is the shared unit of organization: both pipelines process the same ~1,479 series, one extracting content (IUs), the other extracting structure (product hierarchy, VINs, repair times).

## How technical types are identified

Each series' `TechnicalType.perfsql` defines its technical types:

```
tt_id=40, tt_code=HACPU150CVT,       status=1, min=HACPU165*MD206730, max=NULL
tt_id=41, tt_code=HACPU150CVXDRIVE,  status=1, min=HACPU165*MD206730, max=NULL
tt_id=42, tt_code=HACPU165CVT,       status=1, min=HACPU165*MD206730, max=HACPU165*ND201925
tt_id=43, tt_code=HACPU165CVT,       status=1, min=HACPU165*ND201926, max=NULL
```

The `tt_code` is the primary discriminator — different models and variants (e.g., CVT vs CVXDrive) have entirely different codes. Serial number ranges are only needed to disambiguate when the same `tt_code` has multiple `tt_id`s (e.g., tt_id 42 and 43 above — different production runs of the same variant). The `tt_id` is what uniquely identifies each entry.

Only rows with `status=1` are active.

## Wildcard matching in serial ranges

Serial ranges use `*` as a wildcard character. Matching works by:

1. Build a character mask from `min` — positions with `*` are "don't care"
2. Extract only the non-wildcard characters from the VIN, min, and max
3. Compare the masked strings lexicographically

```
VIN:   HACT7210VPD100757
min:   HACT7210*ND101457    (* at position 8 — ignore that position)
masked VIN: HACT7210PD100757
masked min: HACT7210ND101457
```

Additional rules:
- VIN and min must be the same length (otherwise indeterminate)
- `NULL` in min or max → indeterminate (skip)
- If max is `NULL` (but min isn't), the range is open-ended (no upper bound)

## Build process: VIN → TT ID index

The arbortext Rust pipeline (`srt.rs`) processes all 1,479 series to build a VIN lookup index:

1. **Load serial numbers** from `SapSerialNumber.perfsql` (encrypted) — maps VINs to `tt_code`s
2. **Extend to 17-char VINs** via `SapSerialNumber17.perfsql` — some VINs are stored as shorter identifiers
3. **Match to TT ID**: for each VIN, find the `IceTechnicalType` row where:
   - `tt_code` matches the VIN's assigned code
   - The 17-char VIN falls within the `sn_min`–`sn_max` range (wildcard-aware)
   - Exactly one match required (ambiguous → skip)
4. **Shard and write**: hash the last 8 chars of VIN (SHA-256), use first byte as shard key

### Output: VIN shard files

256 TSV files at `vin/{VERSION}/sha256_{00-ff}.tsv`:

```tsv
HACT7210VPD100757	cnh	42
HACT7310MPD200123	cnh	87
```

Format: `{VIN}\tcnh\t{tt_id}` — sorted by VIN within each shard.

### Output: TT code lookup table

`profiles/cnh/{VERSION}/tt_code_lookup.json` — all known TT codes with their serial ranges and product info:

```json
[
  {
    "ttCode": "HACT7210*ND101457",
    "ttId": "42",
    "serialMin": "HACPU165*MD206730",
    "serialMax": "HACPU165*ND201925",
    "brand": "CASE IH",
    "series": "PUMA CVX",
    "model": "PUMA 150 CVX",
    "variant": "CVT"
  }
]
```

Sorted and deduplicated across all series.

### Output: per-TT profile

`profiles/cnh/{VERSION}/{tt_id}/profile.json` — specs and repair operations for one TT:

```json
{
  "specs": {
    "namespace": "cnh",
    "brand": "CASE IH",
    "series": "PUMA CVX",
    "model": "PUMA 150 CVX",
    "variant": "CVT",
    "technicalTypeId": "42",
    "technicalTypeCode": "HACT7210*ND101457",
    "serialMin": "HACPU165*MD206730",
    "serialMax": "HACPU165*ND201925"
  },
  "operations": [
    { "id": "55.640.AA.03", "units": 5, "includes": ["55.640.AB.13"] }
  ]
}
```

## Runtime lookup (app backend)

The backend server (`technical-profile.ts`) resolves VINs in two stages:

### Primary path: shard lookup

```
VIN → SHA-256(last 8 chars) → first byte → shard file on S3
    → scan shard for matching VIN line
    → extract tt_id → load profile from S3
```

This is a direct lookup — O(shard size) scan, ~1/256th of all VINs per shard.

### Fallback: CNH Store API + tt_code_lookup.json

When the VIN isn't in any shard (new machine, data not yet indexed):

1. Call the CNH Store website API with the VIN
2. Response includes a `technicalType` field like `"HACT7210*ND101457 - CVT"`
3. Extract the tt_code (everything before ` - `)
4. Look up tt_code in `tt_code_lookup.json` → may return multiple entries (different serial ranges)
5. If multiple matches, disambiguate using `vinInRange()` with the wildcard serial ranges
6. Result: `tt_id` → load profile from S3

### VRM resolution (UK only)

For UK Vehicle Registration Marks (VRMs like "AB23 XYZ"):

1. Call Vehicle Data Global API → returns VIN
2. Then follow the VIN lookup flow above

### Wildcard matching in TypeScript

The backend reimplements the same wildcard logic as the Rust build:

```typescript
function vinMatchesPattern(vin: string, pattern: string): boolean {
  if (vin.length !== pattern.length) return false;
  for (let i = 0; i < pattern.length; i++) {
    if (pattern[i] === "*") continue;
    if (vin[i] !== pattern[i]) return false;
  }
  return true;
}
```

Note: the TypeScript `vinInRange` checks pattern membership first (does the VIN match the wildcard structure of serialMin?), then does character-by-character comparison at non-wildcard positions for the max bound. This is simpler than the Rust version but equivalent for the data patterns encountered.

## Full resolution chain

```
VIN
 ├─ [primary] SHA-256 shard lookup → tt_id
 └─ [fallback] CNH Store API → tt_code
                                   ↓
                         tt_code_lookup.json
                                   ↓
                    filter by vinInRange(vin, serialMin, serialMax)
                                   ↓
                                tt_id
                                   ↓
                    profiles/cnh/{VERSION}/{tt_id}/
                    ├── profile.json      (specs + operations)
                    ├── srts.tsv          (operation descriptions)
                    └── documentation.json (IU index for MCP)
```

## S3 bucket layout

All reference data lives in the `spectinga-warrantai` S3 bucket:

```
vin/{VERSION}/
  sha256_00.tsv .. sha256_ff.tsv     # 256 VIN shard files

profiles/cnh/{VERSION}/
  tt_code_lookup.json                 # TT code → TT ID mapping
  descriptions.json                   # Operation ID → description(s)
  {tt_id}/
    profile.json                      # Specs + operations for this TT
    srts.tsv                          # Operation index with descriptions
    documentation.json                # IU documentation index (from MCP build)
```
