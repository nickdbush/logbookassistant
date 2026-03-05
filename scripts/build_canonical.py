#!/usr/bin/env python3
"""Build canonical IU set from iu_source_mapping.json.

Resolves multi-hash IU IDs by:
1. Normalizing (strip Arbortext comments + PIs) and re-hashing
2. If all variants collapse to a single normalized hash → trivial dup
3. If distinct after normalization, compute pairwise similarity:
   - >=80% similar → versions/revisions, keep longest
   - <80% similar → genuinely different, keep all with suffixed IDs

Output: data/canonical_iu_mapping.parquet
"""

import json
import re
import hashlib
import zipfile
import sys
import time
from collections import defaultdict
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib.decrypt import decrypt_to_str, zip_path


MAPPING_PATH = Path("data/iu_source_mapping.json")
OUTPUT_PATH = Path("data/canonical_iu_mapping.parquet")

RE_COMMENT = re.compile(r"<!--.*?-->", re.DOTALL)
RE_PI = re.compile(r"<\?.*?\?>", re.DOTALL)


def normalize_xml(xml_str: str) -> str:
    """Strip comments, PIs, and normalize whitespace for comparison."""
    s = RE_COMMENT.sub("", xml_str)
    s = RE_PI.sub("", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def batch_fetch_all(multi_hash: dict) -> dict[str, str]:
    """Fetch all needed variant contents, batched by series to minimize zip opens.

    Returns: {content_hash: xml_string}
    """
    # Build fetch plan: for each content hash, which series/filepath to use
    # (just need one representative per hash)
    fetch_plan = {}  # hash -> (series, filepath)
    for iu_id, variants in multi_hash.items():
        for h, rec in variants:
            if h not in fetch_plan:
                fetch_plan[h] = (rec["appearances"][0]["series"],
                                 rec["appearances"][0]["filepath"])

    # Group by series
    by_series = defaultdict(list)  # series -> [(hash, filepath), ...]
    for h, (series, filepath) in fetch_plan.items():
        by_series[series].append((h, filepath))

    print(f"  Fetching {len(fetch_plan)} variants from {len(by_series)} series zips...")

    contents = {}
    fetched = 0
    failed = 0
    for series_idx, (series, items) in enumerate(by_series.items()):
        if (series_idx + 1) % 100 == 0:
            print(f"    [{series_idx+1}/{len(by_series)} zips] {fetched} fetched, {failed} failed")

        zp = zip_path(series)
        if not zp.exists():
            failed += len(items)
            continue

        try:
            with zipfile.ZipFile(zp, "r") as zf:
                for h, filepath in items:
                    try:
                        raw = zf.read(filepath)
                        contents[h] = decrypt_to_str(raw)
                        fetched += 1
                    except Exception as e:
                        failed += 1
        except Exception as e:
            print(f"    Warning: can't open {zp}: {e}")
            failed += len(items)

    print(f"  Fetched {fetched}, failed {failed}")
    return contents


def resolve_multi_hash(iu_id: str, variants: list[tuple[str, dict]],
                       contents: dict[str, str]) -> list[dict]:
    """Resolve a multi-hash IU ID into one or more canonical entries."""
    # Get fetched content for this IU's variants
    variant_contents = {}
    for h, rec in variants:
        if h in contents:
            variant_contents[h] = contents[h]

    if not variant_contents:
        best = max(variants, key=lambda x: len(x[1]["appearances"]))
        h, rec = best
        app = rec["appearances"][0]
        return [{
            "canonical_id": iu_id,
            "raw_hash": h,
            "iu_type": rec["iu_type"],
            "appearances": json.dumps(
                [a for _, r in variants for a in r["appearances"]]
            ),
            "variant_count": len(variants),
            "source_series": app["series"],
            "source_filepath": app["filepath"],
            "_resolution": "unfetchable",
        }]

    # Normalize and re-hash
    norm_hashes = {}
    for h, xml_str in variant_contents.items():
        norm = normalize_xml(xml_str)
        norm_hashes[h] = hashlib.sha256(norm.encode()).hexdigest()

    unique_norm = set(norm_hashes.values())

    if len(unique_norm) == 1:
        best = max(variants, key=lambda x: len(x[1]["appearances"]))
        h, rec = best
        app = rec["appearances"][0]
        all_appearances = [a for _, r in variants for a in r["appearances"]]
        return [{
            "canonical_id": iu_id,
            "raw_hash": h,
            "iu_type": rec["iu_type"],
            "appearances": json.dumps(all_appearances),
            "variant_count": len(variants),
            "source_series": app["series"],
            "source_filepath": app["filepath"],
            "_resolution": "trivial",
        }]

    # Group variants by normalized hash
    norm_groups = defaultdict(list)
    for h, rec in variants:
        if h in norm_hashes:
            norm_groups[norm_hashes[h]].append((h, rec))
        else:
            first_norm = next(iter(norm_groups)) if norm_groups else list(unique_norm)[0]
            norm_groups[first_norm].append((h, rec))

    group_reps = []
    for norm_h, group in norm_groups.items():
        best = max(group, key=lambda x: len(x[1]["appearances"]))
        h, rec = best
        content = variant_contents.get(h, "")
        all_apps = [a for _, r in group for a in r["appearances"]]
        group_reps.append({
            "hash": h,
            "rec": rec,
            "content": content,
            "all_appearances": all_apps,
            "group": group,
        })

    if len(group_reps) == 1:
        rep = group_reps[0]
        app = rep["rec"]["appearances"][0]
        all_apps = [a for _, r in variants for a in r["appearances"]]
        return [{
            "canonical_id": iu_id,
            "raw_hash": rep["hash"],
            "iu_type": rep["rec"]["iu_type"],
            "appearances": json.dumps(all_apps),
            "variant_count": len(variants),
            "source_series": app["series"],
            "source_filepath": app["filepath"],
            "_resolution": "trivial",
        }]

    # Pairwise Jaccard similarity on whitespace tokens
    token_sets = [set(r["content"].split()) for r in group_reps]
    all_similar = True
    for i in range(len(token_sets)):
        for j in range(i + 1, len(token_sets)):
            intersection = len(token_sets[i] & token_sets[j])
            union = len(token_sets[i] | token_sets[j])
            if union == 0 or intersection / union < 0.80:
                all_similar = False
                break
        if not all_similar:
            break

    if all_similar:
        longest = max(group_reps, key=lambda x: len(x["content"]))
        app = longest["rec"]["appearances"][0]
        all_apps = [a for _, r in variants for a in r["appearances"]]
        return [{
            "canonical_id": iu_id,
            "raw_hash": longest["hash"],
            "iu_type": longest["rec"]["iu_type"],
            "appearances": json.dumps(all_apps),
            "variant_count": len(variants),
            "source_series": app["series"],
            "source_filepath": app["filepath"],
            "_resolution": "version_merge",
        }]

    # Genuinely different — keep all with suffixed IDs
    group_reps.sort(key=lambda x: len(x["content"]), reverse=True)
    results = []
    for idx, rep in enumerate(group_reps, 1):
        suffix = f"_v{idx}"
        app = rep["rec"]["appearances"][0]
        results.append({
            "canonical_id": f"{iu_id}{suffix}",
            "raw_hash": rep["hash"],
            "iu_type": rep["rec"]["iu_type"],
            "appearances": json.dumps(rep["all_appearances"]),
            "variant_count": len(variants),
            "source_series": app["series"],
            "source_filepath": app["filepath"],
            "_resolution": "major_split",
        })
    return results


def main():
    t0 = time.time()
    print("Loading iu_source_mapping.json...")
    with open(MAPPING_PATH) as f:
        mapping = json.load(f)
    print(f"  {len(mapping)} unique content hashes")

    # Group by IU ID
    by_id: dict[str, list[tuple[str, dict]]] = defaultdict(list)
    for h, rec in mapping.items():
        by_id[rec["iu_id"]].append((h, rec))

    single_hash = {k: v for k, v in by_id.items() if len(v) == 1}
    multi_hash = {k: v for k, v in by_id.items() if len(v) > 1}
    print(f"  {len(by_id)} unique IU IDs")
    print(f"  {len(single_hash)} single-hash, {len(multi_hash)} multi-hash")

    # Process single-hash IDs (no file access needed)
    rows = []
    for iu_id, variants in single_hash.items():
        h, rec = variants[0]
        app = rec["appearances"][0]
        rows.append({
            "canonical_id": iu_id,
            "raw_hash": h,
            "iu_type": rec["iu_type"],
            "appearances": json.dumps(rec["appearances"]),
            "variant_count": 1,
            "source_series": app["series"],
            "source_filepath": app["filepath"],
            "_resolution": "single",
        })
    print(f"  {len(rows)} canonical entries from single-hash IDs")

    # Batch-fetch all variant contents (one zip open per series)
    print(f"\nBatch-fetching variant contents for {len(multi_hash)} multi-hash IU IDs...")
    contents = batch_fetch_all(multi_hash)

    # Now resolve each multi-hash IU (all in-memory, fast)
    print(f"\nResolving multi-hash IU IDs...")
    stats = {"trivial": 0, "version_merge": 0, "major_split": 0, "unfetchable": 0}
    multi_items = list(multi_hash.items())
    for i, (iu_id, variants) in enumerate(multi_items):
        if (i + 1) % 10000 == 0:
            print(f"  [{i+1}/{len(multi_items)}]")

        results = resolve_multi_hash(iu_id, variants, contents)
        for r in results:
            stats[r["_resolution"]] += 1
        rows.extend(results)

    elapsed = time.time() - t0
    print(f"\nResolution complete in {elapsed:.0f}s")
    print(f"  Trivial collapses: {stats['trivial']}")
    print(f"  Version merges:    {stats['version_merge']}")
    print(f"  Major splits:      {stats['major_split']} (added {stats['major_split']} extra entries)")
    print(f"  Unfetchable:       {stats['unfetchable']}")

    # Free memory before building dataframe
    del contents, multi_hash, single_hash, by_id, mapping

    # Build dataframe and write parquet
    print(f"\nBuilding parquet with {len(rows)} canonical entries...")
    df = pd.DataFrame(rows)
    df = df.drop(columns=["_resolution"])
    df.to_parquet(OUTPUT_PATH, index=False)
    print(f"  Written to {OUTPUT_PATH}")
    print(f"  File size: {OUTPUT_PATH.stat().st_size / 1e6:.1f} MB")

    # Summary
    print(f"\n=== Final Stats ===")
    print(f"Total canonical IUs: {len(df)}")
    print(f"IU types: {df['iu_type'].value_counts().to_dict()}")
    print(f"Variant count distribution:")
    print(f"  1 variant:  {(df['variant_count'] == 1).sum()}")
    print(f"  2 variants: {(df['variant_count'] == 2).sum()}")
    print(f"  3+ variants: {(df['variant_count'] >= 3).sum()}")
    print(f"Total time: {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
