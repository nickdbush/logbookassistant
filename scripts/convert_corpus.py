#!/usr/bin/env python3
"""Convert all canonical IUs to markdown/HTML and write to parquet.

Input:  data/canonical_iu_mapping.parquet
Output: data/corpus/canonical_ius.parquet

Uses multiprocessing with workers grouped by source_series to minimize
zip reopening. Streams results to parquet via PyArrow to avoid holding
the full corpus in memory.
"""

import json
import sys
import time
import zipfile
from collections import defaultdict
from multiprocessing import Pool
from pathlib import Path
from xml.etree import ElementTree as ET

import markdownify
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib.decrypt import decrypt_to_str, zip_path
from lib.metadata import extract_metadata
from lib.xml_to_html import convert_tree


INPUT_PATH = Path("data/canonical_iu_mapping.parquet")
OUTPUT_DIR = Path("data/corpus")
OUTPUT_PATH = OUTPUT_DIR / "canonical_ius.parquet"
NUM_WORKERS = 10

SCHEMA = pa.schema([
    ("canonical_id", pa.string()),
    ("iu_type", pa.string()),
    ("content_md", pa.string()),
    ("content_html", pa.string()),
    ("metadata", pa.string()),
    ("appearances", pa.string()),
    ("variant_count", pa.int32()),
    ("md_length", pa.int32()),
    ("conversion_error", pa.string()),
])


def process_series_group(args: tuple) -> list[dict]:
    """Process all IUs from a single series zip.

    Args:
        args: (series, list of (canonical_id, filepath, iu_type, appearances, variant_count))

    Returns:
        List of result row dicts.
    """
    series, iu_list = args
    results = []
    zp = zip_path(series)

    if not zp.exists():
        for canonical_id, filepath, iu_type, appearances, variant_count in iu_list:
            results.append({
                "canonical_id": canonical_id,
                "iu_type": iu_type,
                "content_md": None,
                "content_html": None,
                "metadata": None,
                "appearances": appearances,
                "variant_count": variant_count,
                "md_length": 0,
                "conversion_error": f"ZIP not found: {zp}",
            })
        return results

    try:
        zf = zipfile.ZipFile(zp, "r")
    except Exception as e:
        for canonical_id, filepath, iu_type, appearances, variant_count in iu_list:
            results.append({
                "canonical_id": canonical_id,
                "iu_type": iu_type,
                "content_md": None,
                "content_html": None,
                "metadata": None,
                "appearances": appearances,
                "variant_count": variant_count,
                "md_length": 0,
                "conversion_error": f"ZIP open error: {e}",
            })
        return results

    with zf:
        for canonical_id, filepath, iu_type, appearances, variant_count in iu_list:
            try:
                raw = zf.read(filepath)
                xml_str = decrypt_to_str(raw)

                # Parse once, use for both metadata and HTML conversion
                root = ET.fromstring(xml_str)
                meta = extract_metadata(root)
                html = convert_tree(root)

                md = markdownify.markdownify(html, heading_style="ATX")

                results.append({
                    "canonical_id": canonical_id,
                    "iu_type": iu_type,
                    "content_md": md,
                    "content_html": html,
                    "metadata": json.dumps(meta),
                    "appearances": appearances,
                    "variant_count": variant_count,
                    "md_length": len(md),
                    "conversion_error": None,
                })
            except Exception as e:
                results.append({
                    "canonical_id": canonical_id,
                    "iu_type": iu_type,
                    "content_md": None,
                    "content_html": None,
                    "metadata": None,
                    "appearances": appearances,
                    "variant_count": variant_count,
                    "md_length": 0,
                    "conversion_error": f"{type(e).__name__}: {e}",
                })

    return results


def main():
    t0 = time.time()
    print("Loading canonical IU mapping...")
    df = pd.read_parquet(INPUT_PATH)
    total = len(df)
    print(f"  {total} canonical IUs to convert")

    # Group by source_series
    groups = defaultdict(list)
    for row in df.itertuples():
        groups[row.source_series].append((
            row.canonical_id,
            row.source_filepath,
            row.iu_type,
            row.appearances,
            row.variant_count,
        ))

    series_list = list(groups.items())
    print(f"  {len(series_list)} series groups")
    print(f"  Using {NUM_WORKERS} workers")

    # Free the input dataframe
    del df

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Stream results to parquet
    completed = 0
    errors = 0
    writer = None

    try:
        with Pool(NUM_WORKERS) as pool:
            for batch_results in pool.imap_unordered(process_series_group, series_list):
                batch_errors = sum(1 for r in batch_results if r["conversion_error"])
                errors += batch_errors
                completed += len(batch_results)

                # Write batch to parquet
                batch_table = pa.Table.from_pylist(batch_results, schema=SCHEMA)
                if writer is None:
                    writer = pq.ParquetWriter(OUTPUT_PATH, SCHEMA)
                writer.write_table(batch_table)

                if completed % 1000 < len(batch_results) or completed == total:
                    elapsed = time.time() - t0
                    rate = completed / elapsed if elapsed > 0 else 0
                    remaining = (total - completed) / rate if rate > 0 else 0
                    print(
                        f"  [{completed:,}/{total:,}] "
                        f"{rate:.0f} IU/s, "
                        f"ETA {remaining/60:.1f}min, "
                        f"{errors} errors"
                    )
    finally:
        if writer is not None:
            writer.close()

    elapsed = time.time() - t0
    print(f"\nConversion complete in {elapsed/60:.1f} minutes")

    # Summary statistics — read back from parquet (streamed, not all in memory)
    file_size = OUTPUT_PATH.stat().st_size
    print(f"  Written to {OUTPUT_PATH}")
    print(f"  File size: {file_size / 1e9:.2f} GB")

    pf = pq.ParquetFile(OUTPUT_PATH)
    total_rows = pf.metadata.num_rows
    successes = 0
    failures = 0
    md_lengths = []

    for batch in pf.iter_batches(columns=["conversion_error", "md_length"], batch_size=50_000):
        errs = batch.column("conversion_error")
        mds = batch.column("md_length")
        for i in range(len(errs)):
            if errs[i].as_py() is None:
                successes += 1
                md_lengths.append(mds[i].as_py())
            else:
                failures += 1

    print(f"\n=== Summary ===")
    print(f"Total:     {total_rows:,}")
    print(f"Success:   {successes:,}")
    print(f"Failures:  {failures:,} ({failures/total_rows*100:.2f}%)")

    if md_lengths:
        md_lengths.sort()
        n = len(md_lengths)
        print(f"\nMarkdown length distribution (successful):")
        print(f"  Min:    {md_lengths[0]:,}")
        print(f"  Median: {md_lengths[n//2]:,}")
        print(f"  Mean:   {sum(md_lengths)//n:,}")
        print(f"  P95:    {md_lengths[int(n*0.95)]:,}")
        print(f"  Max:    {md_lengths[-1]:,}")

    if failures > 0:
        print(f"\nTop error types:")
        error_counts = defaultdict(int)
        for batch in pf.iter_batches(columns=["conversion_error"], batch_size=50_000):
            for err in batch.column("conversion_error"):
                v = err.as_py()
                if v is not None:
                    error_counts[v.split(":")[0]] += 1
        for err_type, count in sorted(error_counts.items(), key=lambda x: -x[1])[:10]:
            print(f"  {err_type}: {count}")

    print(f"\nTotal time: {elapsed/60:.1f} minutes")


if __name__ == "__main__":
    main()
