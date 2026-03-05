# ICE Codes

## Overview

"ICE code" is overloaded in the CNH Arbortext system — it refers to two distinct things:

1. **Product hierarchy codes** (`A_ICE_CODE` in SQL tables) — sequential IDs within the product tree.
2. **Documentation taxonomy codes** (`cd-ice` attribute on IU XML elements) — classifies content by machine location and information type.

## Product hierarchy codes

Found in `ViewProducts`, `TechnicalType`, and `Model` tables. Sequential identifiers within each parent level:

| Level | Source | Example |
|-------|--------|---------|
| Brand | `ViewProducts.BRAND_ICECODE` | `A` = CASE IH |
| Type | `ViewProducts.TYPE_ICECODE` | `A` = Tractors |
| Product | `ViewProducts.PRODUCT_ICECODE` | `01` = Agricultural |
| Series | `ViewProducts.SERIES_ICECODE` | `034` = PUMA CVX |
| Model | `Model.A_ICE_CODE` | `001` = PUMA 150 CVX |
| Technical Type | `ICE_TECHNICAL_TYPE.A_ICE_CODE` | `002` = CVT |

Series folder name `A.A.01.034` = `Brand.Type.Product.Series` concatenated.

## Documentation taxonomy codes (`cd-ice`)

The `cd-ice` attribute on IU XML root elements. Format: `{LOCATION}-{INFORMATION}`, split on the first `-`. Either side can be empty.

### Location (left of `-`)

`{FAM}.{GRO}[.{SUG}[.{SSG}[.{ASS}]]]` — 2–5 dot-separated segments identifying which machine component the IU covers.

| Segment | WebDocIu column | Description |
|---------|-----------------|-------------|
| FAM (family) | `IU_LOC_FAM` | Machine system (= SAP GROUP_CODE) |
| GRO (group) | `IU_LOC_GRO` | Component (= SAP SUBGROUP_CODE) |
| SUG (subgroup) | `IU_LOC_SUG` | Sub-component |
| SSG (sub-subgroup) | `IU_LOC_SSG` | Further detail |
| ASS (assembly) | `IU_LOC_ASS` | Rare, deepest level |

#### Family codes (= SAP GROUP_CODE)

| Code | Label |
|------|-------|
| 00 | Purchased Protection Plan (PPP) |
| 05 | Machine completion and equipment |
| 10 | Engine |
| 18 | Clutch |
| 21 | Transmission |
| 23 | Four-Wheel Drive (4WD) system |
| 25 | Front axle system |
| 27 | Rear axle system |
| 31 | Power Take-Off (PTO) |
| 33 | Brakes and controls |
| 35 | Hydraulic systems |
| 37 | Hitches, drawbars, and implement couplings |
| 39 | Frames and ballasting |
| 41 | Steering |
| 44 | Wheels |
| 50 | Cab climate control |
| 55 | Electrical systems |
| 82 | Front loader and bucket |
| 88 | Accessories |
| 90 | Platform, cab, bodywork, and decals |

#### Group codes (= SAP SUBGROUP_CODE)

Each family has many groups. Examples for family 55 (Electrical):

| Code | Label |
|------|-------|
| 55.010 | Electroinjector supply wiring harness |
| 55.011 | Fuel level sensor |
| 55.020 | Transmission input speed sensor |
| 55.100 | Alternator |
| 55.350 | Battery |
| 55.525 | Wiper motor |
| 55.640 | Turbocharger speed sensor |

Full group labels come from the first `SAP_MAIN_DESCRIPTION` per group+subgroup in `SRT_SapOperationTranslation.perfsql`.

### Information (right of `-`)

`{TOP}.{SUT}.{CAT}.{INF}` — always 4 dot-separated segments.

| Segment | WebDocIu column | Description |
|---------|-----------------|-------------|
| TOP (topic) | `IU_INF_TOP` | Content type |
| SUT (subtopic) | `IU_INF_SUT` | Subtopic |
| CAT (category) | `IU_INF_CAT` | Category |
| INF (info) | `IU_INF_INF` | Specific info type |

#### Topic codes (TOP)

Counts from series A.A.01.034 (14,099 IUs):

| Code | Count | Content type |
|------|-------|-------------|
| A | 922 | General/Reference — safety rules, kit overviews, maintenance |
| C | 3,406 | Diagrams/Schematics — wiring, hydraulic, exploded views |
| D | 548 | Technical Data — specs, torques, capacities |
| E | 878 | Operation/Controls — usage, settings, display navigation |
| F | 2,519 | Service Procedures — remove/install, calibration |
| G | 5,231 | Fault Diagnosis — error codes, troubleshooting (largest) |
| H | 594 | Bulletins — service/warranty bulletins |
| R | rare | Unknown |

#### Common info code patterns

| Info code | Count | Content |
|-----------|-------|---------|
| A.50.A.10 | 217 | Safety rules |
| A.50.A.52 | 270 | Kit overviews |
| C.10.A.10 | 402 | Component overviews |
| C.20.E.* | ~800 | Wiring schematic sheets |
| C.20.F.* | ~700 | Wire connector diagrams |
| D.20.A.10 | 105 | Technical data sheets |
| F.10.A.15 | 365 | Install procedures |
| F.10.A.19 | 191 | Remove and install procedures |
| F.10.A.25 | 142 | Disassemble and assemble |
| G.30.B.* | ~2,000 | Error code descriptions |
| G.30.F.* | ~1,500 | Fault diagnosis procedures |
| H.10.A.10 | 594 | Service/warranty bulletins |

### Examples

| `cd-ice` | Meaning |
|----------|---------|
| `-A.50.A.10` | General safety rules (no location) |
| `55.640.AG.02-G.30.B.32` | Electrical > Turbo speed sensor — error code description |
| `10.202-F.10.A.19` | Engine > Air cleaner — remove and install |
| `35.204.BQ.03-G.30.C.20` | Hydraulic > Power beyond valve — fault diagnosis |
| `33.220-C.20.B.20` | Brakes > Brake valve line — hydraulic schema |
| `21.507.AD-C.30.A.10` | Transmission > Hydrostat — dynamic description diagram |

## Relationship to SRT codes

Location codes use the **same numbering** as SAP SRT group/subgroup codes:
- `IU_LOC_FAM` = `SAP_GROUP_CODE` (e.g. 55 = Electrical)
- `IU_LOC_GRO` = `SAP_SUBGROUP_CODE` (e.g. 640 = Turbocharger speed sensor)

The deeper location levels (SUG, SSG, ASS) and the information part are documentation-specific — no SRT counterpart. SRT adds `defect_code` and `operation_code` for warranty billing.

## SQL tables using these codes

- **`WEB_DOC_IU`** — maps IUs to documents with `IU_LOC_*`, `IU_INF_*`, and applicability (`IU_APP_MOD`, `IU_APP_TT`)
- **`SRT_SapOperationTranslation`** — human-readable labels for family/group codes
- **`WEBAPPLICABILITY`** — document-level applicability using product hierarchy ICE codes
