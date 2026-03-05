#!/usr/bin/env python3
"""Extract document→IU ordering from doc/ wrapper files in series zips.

Input:  All series zips (doc/ XML files within each)
Output: data/document_structure.parquet

For each document, extracts the ordered list of Master_IU_Ref miuids,
deduplicates across series (same doc_id → one entry with appearances list).
"""

import json
import os
import sys
import time
import zipfile
from collections import defaultdict
from multiprocessing import Pool
from pathlib import Path
from xml.etree import ElementTree as ET

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib.decrypt import decrypt_to_str, SERIES_ROOT

OUTPUT_PATH = Path("data/document_structure.parquet")
NUM_WORKERS = 10

SCHEMA = pa.schema([
    ("document_id", pa.string()),
    ("series", pa.string()),
    ("doc_name", pa.string()),
    ("iu_ids", pa.string()),        # JSON list of miuids in order
    ("num_ius", pa.int32()),
    ("appearances", pa.string()),   # JSON list of series
])


def process_series(series: str) -> list[dict]:
    """Process all doc/ files in a single series zip.

    Returns list of dicts with document_id, series, doc_name, iu_ids.
    """
    zp = SERIES_ROOT / series / "docs.zip"
    if not zp.exists():
        return []

    results = []
    try:
        with zipfile.ZipFile(zp, "r") as zf:
            doc_files = [
                n for n in zf.namelist()
                if n.startswith("doc/") and n.endswith(".xml")
            ]

            for filepath in doc_files:
                try:
                    raw = zf.read(filepath)
                    xml_str = decrypt_to_str(raw)
                    root = ET.fromstring(xml_str)

                    # Document ID from filename (minus .xml)
                    filename = filepath.rsplit("/", 1)[-1]
                    doc_id = filename.removesuffix(".xml")

                    # Document Name attribute
                    doc_name = root.get("Name", "")

                    # Walk tree in document order, collect all Master_IU_Ref miuids
                    iu_ids = []
                    for ref in root.iter("Master_IU_Ref"):
                        miuid = ref.get("miuid", "")
                        if miuid:
                            iu_ids.append(miuid)

                    results.append({
                        "document_id": doc_id,
                        "series": series,
                        "doc_name": doc_name,
                        "iu_ids": iu_ids,
                    })
                except Exception as e:
                    print(f"  FAIL {series}/{filepath}: {e}")
    except Exception as e:
        print(f"  FAIL opening {zp}: {e}")

    return results


def main():
    t0 = time.time()

    # Get all series directories
    series_list = sorted([
        d for d in os.listdir(SERIES_ROOT)
        if os.path.isdir(SERIES_ROOT / d)
    ])
    print(f"Processing {len(series_list)} series for document structure...")

    # Process in parallel
    all_docs = []  # list of (doc_id, series, doc_name, iu_ids)
    completed = 0

    with Pool(NUM_WORKERS) as pool:
        for batch_results in pool.imap_unordered(process_series, series_list):
            all_docs.extend(batch_results)
            completed += 1
            if completed % 100 == 0 or completed == len(series_list):
                elapsed = time.time() - t0
                print(f"  [{completed}/{len(series_list)}] series processed, "
                      f"{len(all_docs)} docs found, {elapsed:.1f}s")

    print(f"\nTotal raw docs: {len(all_docs)}")

    # Deduplicate: group by document_id, merge appearances
    doc_groups = defaultdict(list)
    for doc in all_docs:
        doc_groups[doc["document_id"]].append(doc)

    # Build deduplicated rows
    rows = []
    for doc_id, entries in doc_groups.items():
        # Use first entry as representative
        rep = entries[0]
        appearances = sorted(set(e["series"] for e in entries))

        rows.append({
            "document_id": doc_id,
            "series": rep["series"],
            "doc_name": rep["doc_name"],
            "iu_ids": json.dumps(rep["iu_ids"]),
            "num_ius": len(rep["iu_ids"]),
            "appearances": json.dumps(appearances),
        })

    print(f"Unique documents: {len(rows)}")

    # Write to parquet
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows, schema=SCHEMA)
    pq.write_table(table, OUTPUT_PATH)

    elapsed = time.time() - t0
    file_size = OUTPUT_PATH.stat().st_size
    print(f"\nWritten to {OUTPUT_PATH} ({file_size / 1e6:.1f} MB)")
    print(f"Total time: {elapsed:.1f}s")

    # Report
    print(f"\n=== Document Structure Report ===")
    print(f"Total unique documents: {len(rows):,}")

    num_ius_list = sorted(r["num_ius"] for r in rows)
    n = len(num_ius_list)
    if n > 0:
        print(f"\nIUs per document:")
        print(f"  Min:    {num_ius_list[0]}")
        print(f"  Median: {num_ius_list[n // 2]}")
        print(f"  Mean:   {sum(num_ius_list) // n}")
        print(f"  P95:    {num_ius_list[int(n * 0.95)]}")
        print(f"  Max:    {num_ius_list[-1]}")

    # Appearances distribution
    app_counts = sorted(len(json.loads(r["appearances"])) for r in rows)
    print(f"\nSeries appearances per document:")
    print(f"  Min:    {app_counts[0]}")
    print(f"  Median: {app_counts[n // 2]}")
    print(f"  Mean:   {sum(app_counts) // n}")
    print(f"  Max:    {app_counts[-1]}")

    # Doc name distribution
    name_counts = defaultdict(int)
    for r in rows:
        name_counts[r["doc_name"]] += 1
    print(f"\nDocument types (by Name attribute):")
    for name, count in sorted(name_counts.items(), key=lambda x: -x[1]):
        print(f"  {name:20s}  {count:,}")

    # Empty docs
    empty = sum(1 for r in rows if r["num_ius"] == 0)
    if empty:
        print(f"\nWarning: {empty} documents have zero IUs")


if __name__ == "__main__":
    main()
