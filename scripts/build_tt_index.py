#!/usr/bin/env python3
"""Build IU-to-technical-type applicability index from scripts.zip files.

For each series, parses ViewProducts, TechnicalType, and WebDocIu tables
to determine which IUs apply to each technical type.

Output:
- data/iu_tt_applicability.parquet — deduplicated (iu_miuid, tt_id) pairs
- data/technical_types.parquet — tt_id, tt_code, brand_name, series_name,
                                  model_name, tt_name, series_icecode
"""

import sys
import time
import zipfile
from collections import defaultdict
from multiprocessing import Pool
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib.decrypt import SERIES_ROOT
from lib.perfsql import parse_perfsql

OUTPUT_DIR = Path("data")
APPLICABILITY_PATH = OUTPUT_DIR / "iu_tt_applicability.parquet"
TT_PATH = OUTPUT_DIR / "technical_types.parquet"

DOC_TYPES = ["SM", "OM", "TR", "II", "SB", "AI", "KA", "SG", "WB"]

VP_COLUMNS = [
    "BRAND_NAME", "SERIES_NAME", "SERIES_ICECODE",
    "MODEL_NAME", "MODEL_ICECODE",
    "TT_ID", "TT_ICECODE", "TT_CODE", "TT_NAME",
]
TT_COLUMNS = ["A_ID", "A_CODE", "A_STATUS", "A_MIN", "A_MAX"]
WDI_COLUMNS = ["IU_MASTERIUREF", "IU_APP_MOD", "IU_APP_TT"]


def scripts_zip_path(series: str) -> Path:
    return SERIES_ROOT / series / "scripts.zip"


def process_series(series: str) -> tuple[list[tuple], list[dict]] | None:
    """Process one series, returning (applicability_pairs, tt_info_rows) or None."""
    szp = scripts_zip_path(series)
    if not szp.exists():
        return None

    try:
        with zipfile.ZipFile(szp, "r") as zf:
            names = set(zf.namelist())

            # 1. Parse ViewProducts
            if "common/ViewProducts.perfsql" not in names:
                return None
            vp_rows = parse_perfsql(
                zf.read("common/ViewProducts.perfsql"), columns=VP_COLUMNS
            )
            if not vp_rows:
                return None

            # Group products by tt_id → collect model_icecodes and tt_icecodes
            tt_products = defaultdict(lambda: {
                "model_codes": set(), "tt_codes": set(), "info": None
            })
            for r in vp_rows:
                tt_id = r["TT_ID"]
                entry = tt_products[tt_id]
                if r["MODEL_ICECODE"] is not None:
                    entry["model_codes"].add(r["MODEL_ICECODE"])
                if r["TT_ICECODE"] is not None:
                    entry["tt_codes"].add(r["TT_ICECODE"])
                if entry["info"] is None:
                    entry["info"] = {
                        "tt_id": tt_id,
                        "tt_code": r["TT_CODE"],
                        "brand_name": r["BRAND_NAME"],
                        "series_name": r["SERIES_NAME"],
                        "model_name": r["MODEL_NAME"],
                        "tt_name": r["TT_NAME"],
                        "series_icecode": r["SERIES_ICECODE"],
                    }

            # 2. Parse TechnicalType — filter to active
            if "common/TechnicalType.perfsql" not in names:
                return None
            tt_rows = parse_perfsql(
                zf.read("common/TechnicalType.perfsql"), columns=TT_COLUMNS
            )
            active_tt_ids = set()
            tt_serial_ranges = {}  # A_ID → (A_MIN, A_MAX)
            for r in tt_rows:
                # A_STATUS can be int 1 or string '1'
                if str(r.get("A_STATUS")) == "1":
                    active_tt_ids.add(r["A_ID"])
                    tt_serial_ranges[r["A_ID"]] = (r.get("A_MIN"), r.get("A_MAX"))

            # Filter tt_products to active only, add serial ranges
            active_products = {}
            for tid, v in tt_products.items():
                if tid in active_tt_ids:
                    sn_min, sn_max = tt_serial_ranges.get(tid, (None, None))
                    v["info"]["sn_min"] = sn_min
                    v["info"]["sn_max"] = sn_max
                    active_products[tid] = v
            if not active_products:
                return None

            # 3. Parse all WebDocIu files
            all_wdi_rows = []
            for doc_type in DOC_TYPES:
                path = f"common/{doc_type}/WebDocIu.perfsql"
                if path not in names:
                    continue
                wdi_data = zf.read(path)
                rows = parse_perfsql(wdi_data, columns=WDI_COLUMNS)
                all_wdi_rows.extend(rows)

            if not all_wdi_rows:
                return None

            # 4. For each active TT, filter WebDocIu to applicable IUs
            pairs = set()
            for tt_id, prod in active_products.items():
                model_codes = prod["model_codes"]
                tt_codes = prod["tt_codes"]

                for wdi in all_wdi_rows:
                    iu_app_mod = wdi["IU_APP_MOD"]
                    iu_app_tt = wdi["IU_APP_TT"]

                    # NULL = applies to all
                    mod_ok = iu_app_mod is None or iu_app_mod in model_codes
                    tt_ok = iu_app_tt is None or iu_app_tt in tt_codes

                    if mod_ok and tt_ok:
                        pairs.add((wdi["IU_MASTERIUREF"], tt_id))

            # Collect TT info rows
            tt_info = [prod["info"] for prod in active_products.values()]

            return list(pairs), tt_info

    except Exception as e:
        print(f"  ERROR {series}: {e}", file=sys.stderr)
        return None


def main():
    t0 = time.time()

    # List all series
    series_dirs = sorted(
        d.name for d in SERIES_ROOT.iterdir()
        if d.is_dir() and (d / "scripts.zip").exists()
    )
    print(f"Found {len(series_dirs)} series with scripts.zip")

    # Process in parallel
    all_pairs = set()
    all_tt_info = {}  # tt_id → info dict (dedup across series)
    processed = 0
    skipped = 0

    with Pool(10) as pool:
        for i, result in enumerate(pool.imap_unordered(process_series, series_dirs)):
            if (i + 1) % 100 == 0:
                print(f"  [{i+1}/{len(series_dirs)}] {len(all_pairs)} pairs, "
                      f"{len(all_tt_info)} TTs", file=sys.stderr)

            if result is None:
                skipped += 1
                continue

            pairs, tt_infos = result
            all_pairs.update(pairs)
            for info in tt_infos:
                tt_id = info["tt_id"]
                if tt_id not in all_tt_info:
                    all_tt_info[tt_id] = info
            processed += 1

    elapsed = time.time() - t0
    print(f"\nProcessed {processed} series, skipped {skipped} in {elapsed:.0f}s")
    print(f"  {len(all_pairs):,} unique (iu_miuid, tt_id) pairs")
    print(f"  {len(all_tt_info):,} unique technical types")

    # Write applicability parquet
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    pairs_list = list(all_pairs)
    app_df = pd.DataFrame(pairs_list, columns=["iu_miuid", "tt_id"])
    app_df.to_parquet(APPLICABILITY_PATH, index=False)
    print(f"\nWritten {APPLICABILITY_PATH} ({APPLICABILITY_PATH.stat().st_size / 1e6:.1f} MB)")

    # Write technical types parquet
    tt_df = pd.DataFrame(list(all_tt_info.values()))
    tt_df.to_parquet(TT_PATH, index=False)
    print(f"Written {TT_PATH} ({TT_PATH.stat().st_size / 1e6:.1f} MB)")

    print(f"\nTotal time: {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
