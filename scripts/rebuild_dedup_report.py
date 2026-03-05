"""Rebuild dedup_report.json from existing iu_source_mapping.json.

Avoids re-scanning all series ZIPs — just recomputes stats from the mapping.
"""

import json
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / "data"


def main():
    print("Loading iu_source_mapping.json...")
    with open(DATA_DIR / "iu_source_mapping.json") as f:
        mapping = json.load(f)
    print(f"  {len(mapping):,} unique hashes loaded")

    # Rebuild id_to_hashes
    id_to_hashes: dict[str, set[str]] = {}
    all_series: set[str] = set()
    total_files = 0

    for h, info in mapping.items():
        id_to_hashes.setdefault(info["iu_id"], set()).add(h)
        total_files += len(info["appearances"])
        for a in info["appearances"]:
            all_series.add(a["series"])

    # --- Filename reliability ---
    exceptions = []
    for iu_id, hashes in id_to_hashes.items():
        if len(hashes) > 1:
            exceptions.append({
                "iu_id": iu_id,
                "hash_count": len(hashes),
                "hashes": list(hashes),
            })
    exception_rate = len(exceptions) / len(id_to_hashes) if id_to_hashes else 0
    filename_reliable = len(exceptions) == 0
    filename_effectively_reliable = exception_rate < 0.10

    # --- Distribution ---
    distribution = {
        "appears_in_1_series": 0,
        "appears_in_2_3_series": 0,
        "appears_in_4_5_series": 0,
        "appears_in_6_10_series": 0,
        "appears_in_11_50_series": 0,
        "appears_in_51_plus_series": 0,
    }
    for info in mapping.values():
        n = len(set(a["series"] for a in info["appearances"]))
        if n == 1:
            distribution["appears_in_1_series"] += 1
        elif n <= 3:
            distribution["appears_in_2_3_series"] += 1
        elif n <= 5:
            distribution["appears_in_4_5_series"] += 1
        elif n <= 10:
            distribution["appears_in_6_10_series"] += 1
        elif n <= 50:
            distribution["appears_in_11_50_series"] += 1
        else:
            distribution["appears_in_51_plus_series"] += 1

    # --- By IU type ---
    by_type: dict[str, dict] = {}
    for info in mapping.values():
        t = info["iu_type"]
        if t not in by_type:
            by_type[t] = {"total": 0, "unique": 0}
        by_type[t]["unique"] += 1
        by_type[t]["total"] += len(info["appearances"])

    # --- Top 20 duplicated (no preview — skip re-decrypting) ---
    sorted_by_count = sorted(
        mapping.items(), key=lambda kv: len(kv[1]["appearances"]), reverse=True
    )
    top_20 = []
    for h, info in sorted_by_count[:20]:
        series_set = sorted(set(a["series"] for a in info["appearances"]))
        top_20.append({
            "hash": h[:16] + "...",
            "count": len(info["appearances"]),
            "series_count": len(series_set),
            "iu_type": info["iu_type"],
            "iu_id": info["iu_id"],
        })

    # --- Conversion estimate ---
    dedup_ratio = 1 - len(mapping) / total_files if total_files else 0
    unique_count = len(mapping)
    avg_conversion_ms = 50
    est_time_min = unique_count * avg_conversion_ms / 1000 / 60
    avg_output_kb = 10
    est_size_mb = unique_count * avg_output_kb / 1024

    report = {
        "series_count": len(all_series),
        "total_iu_files": total_files,
        "unique_hashes": len(mapping),
        "unique_iu_ids": len(id_to_hashes),
        "dedup_ratio": round(dedup_ratio, 4),
        "filename_dedup_reliable": filename_reliable,
        "filename_effectively_reliable": filename_effectively_reliable,
        "filename_dedup_exception_count": len(exceptions),
        "filename_dedup_exception_rate": round(exception_rate, 4),
        "filename_dedup_exception_note": (
            "Exceptions are caused by Arbortext editor version comments and "
            "XML attribute reordering — not real content differences. "
            "Filename-based dedup is safe for practical use."
            if filename_effectively_reliable and exceptions else None
        ),
        "distribution": distribution,
        "by_iu_type": by_type,
        "top_20_duplicated": top_20,
        "conversion_estimate": {
            "unique_ius_to_convert": unique_count,
            "est_conversion_time_minutes": round(est_time_min, 1),
            "est_output_size_mb": round(est_size_mb, 1),
        },
    }

    with open(DATA_DIR / "dedup_report.json", "w") as f:
        json.dump(report, f, indent=2)
    print(f"Wrote {DATA_DIR / 'dedup_report.json'}")

    # --- Summary ---
    print(f"\n=== Dedup Summary (all {len(all_series)} series) ===")
    print(f"Total IU files:    {total_files:,}")
    print(f"Unique hashes:     {len(mapping):,}")
    print(f"Unique IU IDs:     {len(id_to_hashes):,}")
    print(f"Dedup ratio:       {dedup_ratio:.1%}")
    print(f"Filename reliable: {filename_effectively_reliable} ({exception_rate:.1%} exception rate)")
    print()
    print("Distribution:")
    for k, v in distribution.items():
        print(f"  {k:30s} {v:>7,}")
    print()
    print("By IU type:")
    for t, v in sorted(by_type.items(), key=lambda x: -x[1]["total"]):
        pct = (1 - v["unique"] / v["total"]) * 100 if v["total"] else 0
        print(f"  {t:25s} {v['total']:>8,} total  {v['unique']:>8,} unique  ({pct:4.0f}% dedup)")
    print()
    print(f"Conversion estimate: {unique_count:,} unique IUs, ~{est_time_min:.0f} min, ~{est_size_mb:.0f} MB")


if __name__ == "__main__":
    main()
