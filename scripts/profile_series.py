"""Profile all XML files in a single series, producing tag_census.json."""

import json
import zipfile
import zlib
import xml.etree.ElementTree as ET
from collections import defaultdict
from Crypto.Cipher import Blowfish

SERIES = "A.A.01.034"
ZIP_PATH = f"/Volumes/logbookdata/cnh/iso/repository/AGCE/data/series/{SERIES}/docs.zip"
KEY = b"\x00" * 24
OUTPUT = "data/tag_census.json"
MAX_SNIPPET_LEN = 200
MAX_SNIPPETS = 3


def unpad_pkcs5(data: bytes) -> bytes:
    pad_len = data[-1]
    if pad_len < 1 or pad_len > 8:
        return data
    if data[-pad_len:] != bytes([pad_len]) * pad_len:
        return data
    return data[:-pad_len]


def decrypt(ciphertext: bytes) -> bytes:
    cipher = Blowfish.new(KEY, Blowfish.MODE_ECB)
    decrypted = cipher.decrypt(ciphertext)
    unpadded = unpad_pkcs5(decrypted)
    return zlib.decompress(unpadded)


def elem_snippet(elem: ET.Element) -> str:
    """Serialize element + immediate children, truncated."""
    s = ET.tostring(elem, encoding="unicode", short_empty_elements=True)
    if len(s) > MAX_SNIPPET_LEN:
        s = s[:MAX_SNIPPET_LEN] + "..."
    return s


def profile_tree(root: ET.Element, tag_counts, tag_snippets, depth_stats, notes):
    """Walk tree collecting tag counts, snippets, and structural notes."""
    stack = [(root, 0)]
    while stack:
        elem, depth = stack.pop()
        tag = elem.tag
        tag_counts[tag] += 1

        if len(tag_snippets[tag]) < MAX_SNIPPETS:
            tag_snippets[tag].append(elem_snippet(elem))

        depth_stats["max"] = max(depth_stats["max"], depth)
        depth_stats["depths"][depth] = depth_stats["depths"].get(depth, 0) + 1

        # Check for mixed content (text + child elements)
        has_text = bool(elem.text and elem.text.strip())
        has_children = len(elem) > 0
        if has_text and has_children:
            notes["mixed_content_tags"].add(tag)

        # Check for tail text (text after closing tag, before next sibling)
        if elem.tail and elem.tail.strip():
            notes["tags_with_tail"].add(tag)

        for child in elem:
            stack.append((child, depth + 1))


def main():
    tag_counts = defaultdict(int)
    tag_snippets = defaultdict(list)
    depth_stats = {"max": 0, "depths": {}}
    notes = {
        "mixed_content_tags": set(),
        "tags_with_tail": set(),
    }

    iu_root_tags = defaultdict(int)  # track what root tags IU files use
    files_processed = 0
    files_failed = 0
    iu_count = 0
    doc_count = 0

    with zipfile.ZipFile(ZIP_PATH) as z:
        all_xml = [n for n in z.namelist() if n.endswith(".xml")]
        # English IUs + doc files only
        xml_files = [
            n for n in all_xml
            if n.startswith("doc/") or n.startswith("iu/EN/")
        ]
        skipped_langs = [n for n in all_xml if n.startswith("iu/") and not n.startswith("iu/EN/")]
        total = len(xml_files)
        print(f"Total XML in zip: {len(all_xml)}")
        print(f"Skipped non-EN IUs: {len(skipped_langs)}")
        print(f"Processing {total} files (doc + iu/EN/) from {SERIES}...")

        for i, name in enumerate(xml_files):
            if i % 5000 == 0 and i > 0:
                print(f"  {i}/{total}...")

            try:
                raw = z.read(name)
                xml_bytes = decrypt(raw)
                text = xml_bytes.decode("utf-8", errors="replace")
                root = ET.fromstring(text)
            except Exception as e:
                files_failed += 1
                if files_failed <= 5:
                    print(f"  FAIL {name}: {e}")
                continue

            files_processed += 1

            if name.startswith("iu/"):
                iu_count += 1
                iu_root_tags[root.tag] += 1
            elif name.startswith("doc/"):
                doc_count += 1

            profile_tree(root, tag_counts, tag_snippets, depth_stats, notes)

    # Build output
    tags_sorted = sorted(tag_counts.items(), key=lambda x: -x[1])

    result = {
        "series": SERIES,
        "files_processed": files_processed,
        "files_failed": files_failed,
        "doc_files": doc_count,
        "iu_files": iu_count,
        "total_unique_tags": len(tag_counts),
        "max_nesting_depth": depth_stats["max"],
        "iu_root_tags": dict(sorted(iu_root_tags.items(), key=lambda x: -x[1])),
        "tags": [
            {
                "tag": tag,
                "count": count,
                "examples": tag_snippets[tag],
            }
            for tag, count in tags_sorted
        ],
        "notes": {
            "mixed_content_tags": sorted(notes["mixed_content_tags"]),
            "tags_with_tail_text": sorted(notes["tags_with_tail"]),
            "depth_distribution": {
                str(k): v
                for k, v in sorted(depth_stats["depths"].items())
            },
        },
    }

    with open(OUTPUT, "w") as f:
        json.dump(result, f, indent=2)

    print(f"\nDone. {files_processed} files processed, {files_failed} failed.")
    print(f"  Doc files: {doc_count}")
    print(f"  IU files: {iu_count}")
    print(f"  Unique tags: {len(tag_counts)}")
    print(f"  Max depth: {depth_stats['max']}")
    print(f"  IU root tags: {dict(iu_root_tags)}")
    print(f"\nTop 20 tags:")
    for tag, count in tags_sorted[:20]:
        print(f"  {tag}: {count}")
    print(f"\nMixed-content tags: {sorted(notes['mixed_content_tags'])}")
    print(f"Output: {OUTPUT}")


if __name__ == "__main__":
    main()
