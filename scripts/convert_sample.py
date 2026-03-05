"""Convert ~50 sample IUs through the full pipeline:
XML → metadata extraction → HTML → Markdown.

Outputs to data/sample_conversions/{IUType}_{filename}/
"""

import json
import re
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path

import markdownify

# Add parent dir to path for lib imports
sys.path.insert(0, str(Path(__file__).resolve().parent))

from lib.decrypt import iter_iu_xmls
from lib.metadata import extract_metadata
from lib.xml_to_html import convert, reset_unknown_tags, get_unknown_tags

SERIES = "A.A.01.034"
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "data" / "sample_conversions"

# How many of each IU type to sample
SAMPLE_TARGETS = {
    "Diagnostic_IU": 4,
    "Service_IU": 8,
    "Functional_Data_IU": 7,
    "Operating_IU": 7,
    "General_IU": 7,
    "Technical_Data_IU": 7,
    "ServiceBulletin_IU": 7,
}

# Additional feature-targeted samples (on top of the type targets)
FEATURE_TARGETS = {
    "fcr": 4,  # Diagnostic_IUs with Fault_Code_Resolution
}


def _strip_pis(xml_str: str) -> str:
    """Strip processing instructions for clean XML parsing."""
    return re.sub(r'<\?Pub[^?]*\?>', '', xml_str)


def get_root_tag(xml_str: str) -> str | None:
    """Parse XML and return root tag name."""
    try:
        root = ET.fromstring(_strip_pis(xml_str))
        return root.tag
    except ET.ParseError:
        return None


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    reset_unknown_tags()

    # Buckets for sampling
    buckets = defaultdict(list)  # tag -> [(filename, xml_str)]
    fcr_bucket = []  # Diagnostic_IUs with Fault_Code_Resolution
    total_target = sum(SAMPLE_TARGETS.values()) + sum(FEATURE_TARGETS.values())

    print(f"Scanning IUs from {SERIES} to select ~{total_target} samples...")

    for filename, xml_str in iter_iu_xmls(SERIES):
        cleaned = _strip_pis(xml_str)
        try:
            root = ET.fromstring(cleaned)
        except ET.ParseError:
            continue
        root_tag = root.tag

        # Feature target: FCR chains
        if (root_tag == "Diagnostic_IU"
                and len(fcr_bucket) < FEATURE_TARGETS["fcr"]
                and root.find(".//Fault_Code_Resolution") is not None):
            fcr_bucket.append((filename, xml_str))
        # Type targets
        elif root_tag in SAMPLE_TARGETS:
            if len(buckets[root_tag]) < SAMPLE_TARGETS[root_tag]:
                buckets[root_tag].append((filename, xml_str))

        # Check if we have enough
        type_done = all(len(buckets[t]) >= n for t, n in SAMPLE_TARGETS.items())
        fcr_done = len(fcr_bucket) >= FEATURE_TARGETS["fcr"]
        if type_done and fcr_done:
            break

    # Merge FCR bucket into Diagnostic_IU
    buckets["Diagnostic_IU"].extend(fcr_bucket)

    total_selected = sum(len(v) for v in buckets.values())
    print(f"Selected {total_selected} IUs:")
    for tag, items in sorted(buckets.items()):
        extra = ""
        if tag == "Diagnostic_IU":
            extra = f" ({len(fcr_bucket)} with FCR chains)"
        print(f"  {tag}: {len(items)}{extra}")

    # Process each IU
    results = []
    for iu_type, items in sorted(buckets.items()):
        for filename, xml_str in items:
            # Create output directory
            stem = Path(filename).stem
            safe_name = f"{iu_type}_{stem}"
            out_dir = OUTPUT_DIR / safe_name
            out_dir.mkdir(parents=True, exist_ok=True)

            result = {"iu_type": iu_type, "filename": filename, "dir": safe_name}

            # 1. Save original XML
            (out_dir / "original.xml").write_text(xml_str, encoding="utf-8")

            # 2. Extract metadata
            try:
                cleaned = _strip_pis(xml_str)
                root = ET.fromstring(cleaned)
                metadata = extract_metadata(root)
                (out_dir / "metadata.json").write_text(
                    json.dumps(metadata, indent=2, ensure_ascii=False),
                    encoding="utf-8",
                )
                result["metadata_ok"] = True
                result["fault_codes"] = len(metadata.get("fault_codes", []))
                result["part_numbers"] = len(metadata.get("part_numbers", []))
                result["fcr_chains"] = len(metadata.get("fcr_chains", []))
            except Exception as e:
                result["metadata_ok"] = False
                result["metadata_error"] = str(e)

            # 3. Convert XML → HTML
            try:
                html = convert(xml_str)
                (out_dir / "intermediate.html").write_text(html, encoding="utf-8")
                result["html_ok"] = True
                result["html_size"] = len(html)
            except Exception as e:
                html = ""
                result["html_ok"] = False
                result["html_error"] = str(e)

            # 4. Convert HTML → Markdown
            try:
                md = markdownify.markdownify(html, heading_style="ATX")
                (out_dir / "final.md").write_text(md, encoding="utf-8")
                result["md_ok"] = True
                result["md_size"] = len(md)
            except Exception as e:
                result["md_ok"] = False
                result["md_error"] = str(e)

            results.append(result)
            status = "OK" if all(result.get(k) for k in ("metadata_ok", "html_ok", "md_ok")) else "ISSUES"
            print(f"  [{status}] {safe_name}")

    # Write conversion report
    unknown = get_unknown_tags()
    report = _build_report(results, unknown)
    report_path = OUTPUT_DIR / "conversion_report.md"
    report_path.write_text(report, encoding="utf-8")
    print(f"\nReport: {report_path}")
    print(f"Unknown tags: {len(unknown)}")
    if unknown:
        top = sorted(unknown.items(), key=lambda x: -x[1])[:20]
        for tag, count in top:
            print(f"  {tag}: {count}")


def _build_report(results, unknown_tags):
    """Build the conversion_report.md content."""
    lines = ["# Sample Conversion Report", ""]

    # Summary
    total = len(results)
    ok = sum(1 for r in results if all(r.get(k) for k in ("metadata_ok", "html_ok", "md_ok")))
    lines.append(f"**Total IUs:** {total}  ")
    lines.append(f"**Fully converted:** {ok}  ")
    lines.append(f"**Unknown tags:** {len(unknown_tags)}")
    lines.append("")

    # Per-type summary
    lines.append("## By IU Type")
    lines.append("")
    by_type = defaultdict(list)
    for r in results:
        by_type[r["iu_type"]].append(r)
    for iu_type, items in sorted(by_type.items()):
        lines.append(f"### {iu_type} ({len(items)} samples)")
        for r in items:
            status = "ok" if all(r.get(k) for k in ("metadata_ok", "html_ok", "md_ok")) else "issues"
            extra = []
            if r.get("fault_codes"):
                extra.append(f"{r['fault_codes']} fault codes")
            if r.get("fcr_chains"):
                extra.append(f"{r['fcr_chains']} FCR chains")
            if r.get("part_numbers"):
                extra.append(f"{r['part_numbers']} part numbers")
            if r.get("html_size"):
                extra.append(f"HTML {r['html_size']}b")
            if r.get("md_size"):
                extra.append(f"MD {r['md_size']}b")
            extra_str = f" — {', '.join(extra)}" if extra else ""
            lines.append(f"- `{r['dir']}` [{status}]{extra_str}")
        lines.append("")

    # Unknown tags
    if unknown_tags:
        lines.append("## Unknown Tags")
        lines.append("")
        lines.append("Tags not in tag_map.yaml (unwrapped with content preserved):")
        lines.append("")
        for tag, count in sorted(unknown_tags.items(), key=lambda x: -x[1]):
            flag = " ⚠️ **needs mapping**" if count > 10 else ""
            lines.append(f"- `{tag}`: {count}{flag}")
        lines.append("")

    # Errors
    errors = [r for r in results if not all(r.get(k) for k in ("metadata_ok", "html_ok", "md_ok"))]
    if errors:
        lines.append("## Errors")
        lines.append("")
        for r in errors:
            lines.append(f"### {r['dir']}")
            for stage in ("metadata", "html", "md"):
                err = r.get(f"{stage}_error")
                if err:
                    lines.append(f"- {stage}: `{err}`")
            lines.append("")

    return "\n".join(lines)


if __name__ == "__main__":
    main()
