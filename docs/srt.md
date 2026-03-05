# Standard Repair Times (SRT)

## Overview

The SRT system defines standardized repair operations with time units for warranty billing. Each technical type has a set of applicable operations, each with a time value. Operations can include sub-operations (inclusion hierarchy) for composite warranty claims.

## Operation code structure

An operation is identified by four parts: `GROUP_CODE.SUBGROUP_CODE.DEFECT_CODE.OPERATION_CODE`

| Part | Description | Example |
|------|-------------|---------|
| GROUP_CODE | Machine system family (same as ICE `IU_LOC_FAM`) | 55 = Electrical |
| SUBGROUP_CODE | Component (same as ICE `IU_LOC_GRO`) | 640 = Turbocharger speed sensor |
| DEFECT_CODE | Defect type (2-letter code) | AA, AB, AC |
| OPERATION_CODE | Specific repair action | 03, 13 |

Example: `55.640.AA.03` = Electrical > Turbo speed sensor > defect AA > operation 03

## SQL tables

### `common/SRT_SapStandardRepairTimes.perfsql`

The core repair time data. Each row is one operation for one SRT code:

| Column | Description |
|--------|-------------|
| `sap_srt_code` | SRT code (links to `SapTechnicalType`) |
| `sap_column_number` | Position in rate table (links to `SapTechnicalType`) |
| `sap_group_code` | Machine system family |
| `sap_subgroup_code` | Component |
| `sap_defect_code` | Defect type |
| `sap_operation_code` | Repair action |
| `sap_repair_time` | Time units (integer) |

### `common/SapTechnicalType.perfsql`

Maps technical types to their SRT code and column:

| Column | Description |
|--------|-------------|
| `sap_srt_code` | SRT code for this TT |
| `sap_column_number` | Column in the rate table |
| `sap_technical_type_code` | TT code (matches `ViewProducts.tt_code`) |

A technical type's operations = all rows in `StandardRepairTime` where `srt_code` and `column_number` match.

### `EN/SRT_SapOperationTranslation.perfsql`

Human-readable operation descriptions (English):

| Column | Description |
|--------|-------------|
| `sap_group_code` | Machine system family |
| `sap_subgroup_code` | Component |
| `sap_defect_code` | Defect type |
| `sap_operation_code` | Repair action |
| `sap_main_description` | Human-readable description |

Keyed by all four code parts. Also serves as the source for family/group labels (first description per group+subgroup).

### `common/SRT_SAPAdditionalInfo.perfsql`

Defines parent operations that include sub-operations:

| Column | Description |
|--------|-------------|
| `add_info_id` | Link ID for inclusion |
| `sap_group_code` | Parent operation group |
| `sap_subgroup_code` | Parent operation subgroup |
| `sap_defect_code` | Parent operation defect |
| `sap_operation_code` | Parent operation action |

### `common/SRT_SAPAdditionalInfoContent.perfsql`

Child operations included in a parent:

| Column | Description |
|--------|-------------|
| `add_info_id` | Links to `AdditionalInfo.add_info_id` |
| `add_inc_status` | "I" = included (filter on this) |
| `sap_group_code` | Child operation group |
| `sap_subgroup_code` | Child operation subgroup |
| `sap_defect_code` | Child operation defect |
| `sap_operation_code` | Child operation action |

## Inclusion hierarchy

Parent operations can bundle multiple child operations for warranty claims:

1. Look up parent operation in `AdditionalInfo` → get `add_info_id`(s)
2. Look up `add_info_id` in `AdditionalInfoContent` where `inc_status = "I"`
3. Collect child operations (deduplicate, sort)

The parent's warranty time covers the sum of included operations.

## Linking TT to operations

```
TechnicalType.tt_code
    → SapTechnicalType → (srt_code, column_number)
    → StandardRepairTime (filter by srt_code + column_number) → operations list
    → OperationTranslation → human-readable descriptions
    → AdditionalInfo + AdditionalInfoContent → inclusion hierarchy
```

## Output format (from arbortext Rust pipeline)

Per technical type, a `profile.json`:
```json
{
  "specs": {
    "namespace": "cnh",
    "brand": "CASE IH",
    "series": "PUMA CVX",
    "model": "PUMA 150 CVX",
    "variant": "CVT",
    "technicalTypeId": "12345",
    "technicalTypeCode": "ABC",
    "serialMin": "HACT7210NND101457",
    "serialMax": null
  },
  "operations": [
    {
      "id": "55.640.AA.03",
      "units": 5,
      "includes": ["55.640.AB.13", "55.640.AC.03"]
    }
  ]
}
```

And a `srts.tsv` with operation index and descriptions.

## VIN sharding (for lookup)

VINs are sharded across 256 files for scalable lookup:
- Hash last 8 characters (VIS portion) with SHA-256
- Use first byte as shard key: `sha256_{00-ff}.tsv`
- Each shard is a sorted TSV: `VIN\tcnh\tTT_ID`
