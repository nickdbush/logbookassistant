# scripts.zip Data Model

Each series folder contains a `scripts.zip` alongside `docs.zip`. The scripts zip holds UTF-16BE encoded `.perfsql` files containing SQL `INSERT INTO ... VALUES` statements. These define the relational data that links IUs to documents, models, technical types, VINs, and repair operations.

All scripts are unencrypted except `SapSerialNumber.perfsql` (same Blowfish ECB as IU files).

## Tables

### Product hierarchy

**`common/ViewProducts.perfsql`** — Master product catalog. One row per technical type, with full hierarchy:

| Column | Description |
|--------|-------------|
| `brand_name` | e.g. "CASE IH" |
| `series_name` | e.g. "PUMA CVX" |
| `model_name` | e.g. "PUMA 150 CVX" |
| `model_icecode` | Product hierarchy ICE code for model |
| `tt_id` | Internal technical type ID (numeric) |
| `tt_icecode` | Product hierarchy ICE code for TT |
| `tt_code` | Technical type code (string) |
| `tt_name` | Technical type variant name |
| `tt_sn_min` | Serial number range start |
| `tt_sn_max` | Serial number range end ("NULL" = no upper bound) |

**`common/TechnicalType.perfsql`** — Technical type definitions with serial ranges and status:

| Column | Description |
|--------|-------------|
| `a_id` | TT ID (matches `ViewProducts.tt_id`) |
| `a_code` | TT code (matches `ViewProducts.tt_code`) |
| `a_status` | "1" = active (skip others) |
| `a_min` | Serial number min (may have trailing " -") |
| `a_max` | Serial number max (nullable) |

### VIN resolution

**`common/SapSerialNumber.perfsql`** (ENCRYPTED) — Maps VINs to technical type codes:

| Column | Description |
|--------|-------------|
| `sap_vin` | Vehicle identification number |
| `sap_technical_type_code` | TT code for this VIN |

**`common/SapSerialNumber17.perfsql`** — Maps short VINs to 17-character VINs:

| Column | Description |
|--------|-------------|
| `sap_vin` | Short VIN |
| `sap_vin_17` | Full 17-character VIN |

#### VIN range matching

Serial ranges in `TechnicalType` use `*` as wildcard masks. To check if a VIN falls in range:
1. Build a mask from `min` — positions with `*` are ignored
2. Extract non-wildcard chars from VIN, min, and max
3. Compare masked strings lexicographically
4. VIN and min must be the same length (else indeterminate)
5. "NULL" in min or max → indeterminate

Example: VIN `HACT7210VPD100757` matches range `HACT7210*ND101457` (the `*` at position 8 is ignored).

### IU-to-document mapping

**`common/{TYPE}/WebDocIu.perfsql`** — One file per document type (SB, OM, II, TR, AI, KA, SG, WB). Maps IUs to documents with applicability and classification:

| Column | Description |
|--------|-------------|
| `iu_masteriuref` | IU miuid |
| `iu_app_mod` | Model ICE code applicability (nullable = all models) |
| `iu_app_tt` | TT ICE code applicability (nullable = all TTs) |
| `iu_loc_fam` | Location family (= SAP GROUP_CODE) |
| `iu_loc_gro` | Location group (= SAP SUBGROUP_CODE) |
| `iu_inf_top` | Topic code (A/C/D/E/F/G/H) |
| `iu_fcrtitle` | Title reference ID |

### Titles and headers

**`EN/Titles.perfsql`** — IU title lookup:

| Column | Description |
|--------|-------------|
| `id_title` | Title ID (referenced by `iu_fcrtitle`) |
| `title` | Human-readable title text |

**`EN/SB/WebSbHeader.perfsql`** — Service bulletin metadata:

| Column | Description |
|--------|-------------|
| `web_sb_id` | Service bulletin ID |
| `web_subject` | Bulletin subject line |

### Title resolution priority

1. Title from parsed XML (`<Full_Title>` text content)
2. Title from `Titles.perfsql` via `iu_fcrtitle` reference
3. Fallback to miuid

## Relationships

```
VIN → SapSerialNumber → tt_code
                              ↓
                    TechnicalType (filter status="1", range match)
                              ↓
                          tt_id → ViewProducts → brand, series, model
                              ↓
                    SapTechnicalType → srt_code, column_number
                              ↓
                    StandardRepairTime → repair operations
                              ↓
                    WebDocIu (filter by tt applicability) → IU miuids
```
