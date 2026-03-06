#!/usr/bin/env python3
"""Enrich canonical IU corpus: flatten metadata JSON, add derived columns, profile.

Input:  data/corpus/canonical_ius.parquet
Output: data/corpus/canonical_ius_enriched.parquet

Steps:
  1. Stream parquet, parse metadata JSON, flatten into columns, add derived fields
  2. Profile the enriched corpus (coverage stats, cross-ref graph, large IUs)
"""

import json
import re
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

INPUT_PATH = Path("data/corpus/canonical_ius.parquet")
OUTPUT_PATH = Path("data/corpus/canonical_ius_enriched.parquet")

# Metadata fields to extract as JSON-string columns
META_LIST_FIELDS = [
    "fault_codes",
    "part_numbers",
    "tool_references",
    "consumable_references",
    "warranty_codes",
    "iu_cross_references",
    "configuration",
    "fcr_chains",
]

OUTPUT_SCHEMA = pa.schema([
    ("canonical_id", pa.string()),
    ("content_type", pa.string()),
    ("content_md", pa.string()),
    ("content_html", pa.string()),
    ("appearances", pa.string()),
    ("variant_count", pa.int32()),
    ("md_length", pa.int32()),
    ("conversion_error", pa.string()),
    # Flattened metadata
    ("fault_codes", pa.string()),
    ("part_numbers", pa.string()),
    ("tool_references", pa.string()),
    ("consumable_references", pa.string()),
    ("warranty_codes", pa.string()),
    ("iu_cross_references", pa.string()),
    ("configuration", pa.string()),
    ("fcr_chains", pa.string()),
    # Derived
    ("title", pa.string()),
    ("has_tables", pa.bool_()),
    ("has_images", pa.bool_()),
    ("estimated_tokens", pa.int32()),
])

BATCH_SIZE = 10_000


def enrich_batch(batch: pa.RecordBatch) -> list[dict]:
    """Process a batch of rows, returning enriched dicts."""
    rows = batch.to_pydict()
    n = len(rows["canonical_id"])
    results = []

    for i in range(n):
        meta_str = rows["metadata"][i]
        meta = {}
        if meta_str:
            try:
                meta = json.loads(meta_str)
            except (json.JSONDecodeError, TypeError):
                pass

        content_html = rows["content_html"][i] or ""
        content_md = rows["content_md"][i] or ""

        # Extract title from first markdown heading
        title = ""
        heading_match = re.match(r"^#+\s+(.+)", content_md)
        if heading_match:
            title = heading_match.group(1).strip()

        row = {
            "canonical_id": rows["canonical_id"][i],
            "content_type": rows["iu_type"][i],
            "content_md": rows["content_md"][i],
            "content_html": rows["content_html"][i],
            "appearances": rows["appearances"][i],
            "variant_count": rows["variant_count"][i],
            "md_length": rows["md_length"][i],
            "conversion_error": rows["conversion_error"][i],
            # Derived
            "title": title,
            "has_tables": "<table" in content_html.lower(),
            "has_images": "<img" in content_html.lower(),
            "estimated_tokens": len(content_md) // 4,
        }

        # Flatten metadata list fields
        for field in META_LIST_FIELDS:
            val = meta.get(field, [])
            if field == "part_numbers" and val:
                # Flatten to just part number strings
                val = [
                    item["part_number"] if isinstance(item, dict) else item
                    for item in val
                ]
            row[field] = json.dumps(val) if val else "[]"

        results.append(row)

    return results


def step1_enrich():
    """Flatten metadata and write enriched parquet."""
    t0 = time.time()
    pf = pq.ParquetFile(INPUT_PATH)
    total = pf.metadata.num_rows
    print(f"Enriching {total:,} IUs from {INPUT_PATH}")

    writer = None
    completed = 0

    try:
        for batch in pf.iter_batches(batch_size=BATCH_SIZE):
            enriched = enrich_batch(batch)
            table = pa.Table.from_pylist(enriched, schema=OUTPUT_SCHEMA)
            if writer is None:
                writer = pq.ParquetWriter(OUTPUT_PATH, OUTPUT_SCHEMA)
            writer.write_table(table)
            completed += len(enriched)

            if completed % 50_000 < BATCH_SIZE or completed == total:
                elapsed = time.time() - t0
                rate = completed / elapsed if elapsed > 0 else 0
                print(f"  [{completed:,}/{total:,}] {rate:.0f} IU/s")
    finally:
        if writer is not None:
            writer.close()

    elapsed = time.time() - t0
    file_size = OUTPUT_PATH.stat().st_size
    print(f"\nEnrichment complete in {elapsed:.1f}s")
    print(f"  Output: {OUTPUT_PATH} ({file_size / 1e9:.2f} GB)")


def step2_profile():
    """Profile the enriched corpus."""
    print("\n=== Corpus Profile ===\n")
    pf = pq.ParquetFile(OUTPUT_PATH)
    total = pf.metadata.num_rows

    # Coverage stats
    field_counts = {f: 0 for f in META_LIST_FIELDS}
    field_counts["has_tables"] = 0
    field_counts["has_images"] = 0

    # Cross-ref stats
    total_xrefs = 0
    ius_with_xrefs = 0

    # Large IU tracking
    large_ius = []

    # Token stats
    total_tokens = 0

    for batch in pf.iter_batches(batch_size=BATCH_SIZE):
        rows = batch.to_pydict()
        n = len(rows["canonical_id"])

        for i in range(n):
            # Coverage
            for field in META_LIST_FIELDS:
                val = rows[field][i]
                if val and val != "[]":
                    field_counts[field] += 1

            if rows["has_tables"][i]:
                field_counts["has_tables"] += 1
            if rows["has_images"][i]:
                field_counts["has_images"] += 1

            # Cross-refs
            xrefs = rows["iu_cross_references"][i]
            if xrefs and xrefs != "[]":
                xref_list = json.loads(xrefs)
                total_xrefs += len(xref_list)
                ius_with_xrefs += 1

            # Tokens
            total_tokens += rows["estimated_tokens"][i] or 0

            # Large IUs
            md_len = rows["md_length"][i] or 0
            if md_len > 50_000:
                content_md = rows["content_md"][i] or ""
                snippet = content_md[:200].replace("\n", " ")
                large_ius.append({
                    "canonical_id": rows["canonical_id"][i],
                    "content_type": rows["content_type"][i],
                    "md_length": md_len,
                    "snippet": snippet,
                })

    # Print coverage
    print("Field coverage:")
    for field, count in field_counts.items():
        pct = count / total * 100
        print(f"  {field:30s}  {count:>8,}  ({pct:5.1f}%)")

    # Print cross-ref stats
    print(f"\nCross-reference graph:")
    print(f"  IUs with cross-refs: {ius_with_xrefs:,} ({ius_with_xrefs/total*100:.1f}%)")
    if ius_with_xrefs > 0:
        print(f"  Total cross-refs: {total_xrefs:,}")
        print(f"  Avg refs/IU (among those with refs): {total_xrefs/ius_with_xrefs:.1f}")

    # Print token stats
    print(f"\nEstimated tokens:")
    print(f"  Total: {total_tokens:,}")
    print(f"  Avg per IU: {total_tokens // total:,}")

    # Print large IUs
    print(f"\nLarge IUs (>50K chars): {len(large_ius)}")
    large_ius.sort(key=lambda x: -x["md_length"])
    for iu in large_ius[:20]:
        print(f"  {iu['canonical_id']:>12s}  {iu['content_type']:25s}  "
              f"{iu['md_length']:>8,} chars  {iu['snippet'][:80]}...")
    if len(large_ius) > 20:
        print(f"  ... and {len(large_ius) - 20} more")


def main():
    step1_enrich()
    step2_profile()


if __name__ == "__main__":
    main()
