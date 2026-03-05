#!/usr/bin/env python3
"""Chunk the enriched IU corpus for RAG retrieval.

Input:  data/corpus/canonical_ius_enriched.parquet
Output: data/corpus/chunks.parquet

Strategy:
  - IUs ≤2000 estimated tokens: keep whole (one chunk = one IU)
  - IUs >2000 tokens: split on heading boundaries, fallback to paragraph split
  - Never split mid-table or mid-list-item
  - Prepend ancestor headings as context prefix to each chunk
"""

import re
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

INPUT_PATH = Path("data/corpus/canonical_ius_enriched.parquet")
OUTPUT_PATH = Path("data/corpus/chunks.parquet")

MAX_TOKENS = 2000
MIN_TOKENS = 100
FALLBACK_TARGET = 1500
OVERLAP_CHARS = 200
MIN_CHUNK_TOKENS = 10  # Drop chunks below this (heading-only stubs)
BATCH_SIZE = 10_000

HEADING_RE = re.compile(r"^(#{1,6})\s", re.MULTILINE)

OUTPUT_SCHEMA = pa.schema([
    ("chunk_id", pa.string()),
    ("canonical_iu_id", pa.string()),
    ("chunk_index", pa.int32()),
    ("num_chunks", pa.int32()),
    ("text", pa.string()),
    ("token_count", pa.int32()),
    ("content_type", pa.string()),
    ("fault_codes", pa.string()),
    ("part_numbers", pa.string()),
    ("tool_references", pa.string()),
])


def _est_tokens(text: str) -> int:
    return len(text) // 4


def _parse_sections(md_text: str) -> list[tuple[int, str, str]]:
    """Parse markdown into sections: (heading_level, heading_line, body).

    Content before the first heading gets level=0, heading_line="".
    """
    sections = []
    matches = list(HEADING_RE.finditer(md_text))

    if not matches:
        return [(0, "", md_text)]

    # Content before first heading
    if matches[0].start() > 0:
        preamble = md_text[: matches[0].start()].strip()
        if preamble:
            sections.append((0, "", preamble))

    for i, m in enumerate(matches):
        level = len(m.group(1))
        # Find heading line end
        line_end = md_text.find("\n", m.start())
        if line_end == -1:
            heading_line = md_text[m.start():]
            body = ""
        else:
            heading_line = md_text[m.start() : line_end]
            # Body runs to next heading
            if i + 1 < len(matches):
                body = md_text[line_end + 1 : matches[i + 1].start()].strip()
            else:
                body = md_text[line_end + 1 :].strip()

        sections.append((level, heading_line, body))

    return sections


def _is_table_or_list_line(line: str) -> bool:
    """Check if a line is part of a table or list continuation."""
    stripped = line.strip()
    return stripped.startswith("|") or stripped.startswith("- ") or bool(
        re.match(r"^\d+\.\s", stripped)
    )


def _find_para_break(text: str, target_pos: int) -> int:
    """Find nearest double-newline break near target_pos, avoiding table/list splits."""
    # Search for double-newline breaks
    breaks = [m.start() for m in re.finditer(r"\n\n", text)]
    if not breaks:
        return target_pos

    # Find closest break to target
    best = min(breaks, key=lambda b: abs(b - target_pos))

    # Verify we're not splitting inside a table or list
    # Check the line after the break
    after = text[best + 2 : best + 100] if best + 2 < len(text) else ""
    first_line = after.split("\n")[0] if after else ""

    # If we'd be splitting into a table continuation, search for a better break
    if first_line.strip().startswith("|"):
        # Find a break that's NOT followed by a table line
        safe_breaks = [
            b
            for b in breaks
            if b < target_pos + 2000
            and not (text[b + 2 : b + 10].strip().startswith("|") if b + 2 < len(text) else False)
        ]
        if safe_breaks:
            best = min(safe_breaks, key=lambda b: abs(b - target_pos))

    return best


def _split_table_rows(text: str, context_prefix: str) -> list[str]:
    """Split a block of table rows into chunks at row boundaries."""
    lines = text.split("\n")
    target_chars = FALLBACK_TARGET * 4
    max_chars = MAX_TOKENS * 4

    # Find table header (first row + separator row like |---|---|)
    table_header = ""
    header_end = 0
    for i, line in enumerate(lines):
        if re.match(r"^\s*\|[\s\-:|]+\|\s*$", line):
            # This is the separator — header is everything up to and including it
            table_header = "\n".join(lines[: i + 1])
            header_end = i + 1
            break

    chunks = []
    current_lines = []
    current_len = 0

    for line in lines[header_end:]:
        line_len = len(line) + 1  # +1 for newline
        if current_len + line_len > target_chars and current_len > 0:
            # Emit chunk
            body = "\n".join(current_lines)
            if table_header:
                body = table_header + "\n" + body
            prefix = context_prefix + "\n\n" if context_prefix else ""
            chunks.append(prefix + body)
            current_lines = []
            current_len = 0

        current_lines.append(line)
        current_len += line_len

    # Remainder
    if current_lines:
        body = "\n".join(current_lines)
        if table_header:
            body = table_header + "\n" + body
        prefix = context_prefix + "\n\n" if context_prefix else ""
        chunks.append(prefix + body)

    return chunks if chunks else [text]


def _fallback_split(text: str, context_prefix: str) -> list[str]:
    """Split text on paragraph or table-row boundaries with overlap."""
    # Check if this is predominantly a table (>50% table lines)
    lines = text.split("\n")
    table_lines = sum(1 for l in lines if l.strip().startswith("|"))
    if table_lines > len(lines) * 0.5:
        return _split_table_rows(text, context_prefix)

    target_chars = FALLBACK_TARGET * 4  # tokens → chars
    chunks = []
    pos = 0

    while pos < len(text):
        if len(text) - pos <= (MAX_TOKENS * 4):
            # Remaining text fits in one chunk
            chunk_text = text[pos:]
            if chunk_text.strip():
                if chunks:
                    chunks.append(context_prefix + "\n\n" + chunk_text.strip())
                else:
                    chunks.append(chunk_text.strip())
            break

        # Find split point
        split_at = _find_para_break(text, pos + target_chars)
        if split_at <= pos:
            split_at = pos + target_chars  # Force progress

        chunk_text = text[pos:split_at].strip()
        if chunk_text:
            if chunks:
                chunks.append(context_prefix + "\n\n" + chunk_text)
            else:
                chunks.append(chunk_text)

        # Move forward with overlap
        overlap_start = max(pos, split_at - OVERLAP_CHARS)
        pos = split_at
        # Don't overlap beyond the split point for the next chunk
        if pos < len(text) and OVERLAP_CHARS > 0:
            # Find a clean break point for overlap start
            overlap_break = text.rfind("\n\n", overlap_start, split_at)
            if overlap_break > overlap_start:
                pos = overlap_break + 2

    return chunks if chunks else [text]


def split_on_headings(md_text: str) -> list[str]:
    """Split markdown on heading boundaries with context prefixes."""
    sections = _parse_sections(md_text)

    if len(sections) <= 1:
        # No headings or single section — use fallback if too large
        if _est_tokens(md_text) > MAX_TOKENS:
            return _fallback_split(md_text, "")
        return [md_text]

    # Track ancestor headings for context
    ancestors = {}  # level → heading_line
    raw_chunks = []  # (context_prefix, text)

    for level, heading_line, body in sections:
        if level > 0:
            # Update ancestors: this heading replaces any same-or-lower level
            ancestors[level] = heading_line
            # Remove descendants
            for k in list(ancestors.keys()):
                if k > level:
                    del ancestors[k]

        # Build context prefix from ancestors above current level
        if level > 0:
            prefix_parts = [
                ancestors[k] for k in sorted(ancestors.keys()) if k < level
            ]
        else:
            prefix_parts = []

        context_prefix = "\n\n".join(prefix_parts) if prefix_parts else ""

        # Build section text
        if heading_line and body:
            section_text = heading_line + "\n\n" + body
        elif heading_line:
            section_text = heading_line
        else:
            section_text = body

        if context_prefix:
            full_text = context_prefix + "\n\n" + section_text
        else:
            full_text = section_text

        raw_chunks.append((context_prefix, section_text, full_text))

    # Merge tiny sections with next sibling
    merged = []
    i = 0
    while i < len(raw_chunks):
        ctx, sec, full = raw_chunks[i]
        if _est_tokens(full) < MIN_TOKENS and i + 1 < len(raw_chunks):
            # Merge with next
            next_ctx, next_sec, next_full = raw_chunks[i + 1]
            combined_sec = sec + "\n\n" + next_sec
            combined_full = (ctx + "\n\n" + combined_sec) if ctx else combined_sec
            raw_chunks[i + 1] = (next_ctx or ctx, combined_sec, combined_full)
            i += 1
            continue
        merged.append((ctx, sec, full))
        i += 1

    # Split oversized sections with fallback
    final_chunks = []
    for ctx, sec, full in merged:
        if _est_tokens(full) > MAX_TOKENS:
            sub_chunks = _fallback_split(full, ctx)
            final_chunks.extend(sub_chunks)
        else:
            final_chunks.append(full)

    # Filter empty/trivial chunks
    return [c for c in final_chunks if c.strip() and _est_tokens(c) >= MIN_CHUNK_TOKENS]


def chunk_iu(row: dict) -> list[dict]:
    """Chunk a single IU, returning list of chunk dicts."""
    canonical_id = row["canonical_iu_id"]
    content_md = row["text_source"] or ""
    est_tokens = row["est_tokens"] or 0

    inherited = {
        "canonical_iu_id": canonical_id,
        "content_type": row["content_type"],
        "fault_codes": row["fault_codes"],
        "part_numbers": row["part_numbers"],
        "tool_references": row["tool_references"],
    }

    if est_tokens < MIN_CHUNK_TOKENS or not content_md.strip():
        return []  # Skip trivial/empty IUs

    if est_tokens <= MAX_TOKENS:
        # Tier 1: keep whole
        return [{
            "chunk_id": f"{canonical_id}_c000",
            "chunk_index": 0,
            "num_chunks": 1,
            "text": content_md,
            "token_count": est_tokens,
            **inherited,
        }]

    # Tier 2: split
    texts = split_on_headings(content_md)
    num_chunks = len(texts)

    return [
        {
            "chunk_id": f"{canonical_id}_c{i:03d}",
            "chunk_index": i,
            "num_chunks": num_chunks,
            "text": t,
            "token_count": _est_tokens(t),
            **inherited,
        }
        for i, t in enumerate(texts)
    ]


def main():
    t0 = time.time()
    pf = pq.ParquetFile(INPUT_PATH)
    total_ius = pf.metadata.num_rows
    print(f"Chunking {total_ius:,} IUs from {INPUT_PATH}")

    writer = None
    completed = 0
    total_chunks = 0
    unsplit = 0
    split_ius = 0
    skipped = 0

    try:
        for batch in pf.iter_batches(
            batch_size=BATCH_SIZE,
            columns=[
                "canonical_id", "content_type", "content_md",
                "estimated_tokens", "fault_codes", "part_numbers",
                "tool_references",
            ],
        ):
            rows = batch.to_pydict()
            n = len(rows["canonical_id"])
            batch_chunks = []

            for i in range(n):
                row = {
                    "canonical_iu_id": rows["canonical_id"][i],
                    "content_type": rows["content_type"][i],
                    "text_source": rows["content_md"][i],
                    "est_tokens": rows["estimated_tokens"][i],
                    "fault_codes": rows["fault_codes"][i],
                    "part_numbers": rows["part_numbers"][i],
                    "tool_references": rows["tool_references"][i],
                }
                chunks = chunk_iu(row)
                batch_chunks.extend(chunks)

                if len(chunks) == 0:
                    skipped += 1
                elif len(chunks) == 1:
                    unsplit += 1
                else:
                    split_ius += 1

            # Write batch
            table = pa.Table.from_pylist(batch_chunks, schema=OUTPUT_SCHEMA)
            if writer is None:
                writer = pq.ParquetWriter(OUTPUT_PATH, OUTPUT_SCHEMA)
            writer.write_table(table)

            total_chunks += len(batch_chunks)
            completed += n

            if completed % 50_000 < BATCH_SIZE or completed == total_ius:
                elapsed = time.time() - t0
                rate = completed / elapsed if elapsed > 0 else 0
                print(
                    f"  [{completed:,}/{total_ius:,}] {rate:.0f} IU/s, "
                    f"{total_chunks:,} chunks so far"
                )
    finally:
        if writer is not None:
            writer.close()

    elapsed = time.time() - t0
    file_size = OUTPUT_PATH.stat().st_size
    print(f"\nChunking complete in {elapsed:.1f}s")
    print(f"  Output: {OUTPUT_PATH} ({file_size / 1e9:.2f} GB)")

    # === Report ===
    print(f"\n=== Chunk Report ===")
    print(f"Total IUs:       {total_ius:,}")
    print(f"Skipped (<{MIN_CHUNK_TOKENS} tokens): {skipped:,}")
    print(f"Unsplit IUs:     {unsplit:,} ({unsplit/total_ius*100:.1f}%)")
    print(f"Split IUs:       {split_ius:,} ({split_ius/total_ius*100:.1f}%)")
    print(f"Total chunks:    {total_chunks:,}")
    print(f"Expansion ratio: {total_chunks/total_ius:.2f}x")

    # Token distribution from output
    print(f"\nChunk token distribution:")
    token_counts = []
    max_chunks_per_iu = []  # (num_chunks, canonical_id)
    total_tokens = 0

    pf_out = pq.ParquetFile(OUTPUT_PATH)
    for batch in pf_out.iter_batches(
        batch_size=50_000,
        columns=["token_count", "num_chunks", "canonical_iu_id", "chunk_index"],
    ):
        rows = batch.to_pydict()
        for i in range(len(rows["token_count"])):
            tc = rows["token_count"][i]
            token_counts.append(tc)
            total_tokens += tc
            if rows["chunk_index"][i] == 0 and rows["num_chunks"][i] > 1:
                max_chunks_per_iu.append(
                    (rows["num_chunks"][i], rows["canonical_iu_id"][i])
                )

    token_counts.sort()
    n = len(token_counts)
    print(f"  Min:    {token_counts[0]:,}")
    print(f"  Median: {token_counts[n//2]:,}")
    print(f"  Mean:   {total_tokens//n:,}")
    print(f"  P95:    {token_counts[int(n*0.95)]:,}")
    print(f"  Max:    {token_counts[-1]:,}")
    print(f"  Total:  {total_tokens:,}")

    # Top 10 IUs by chunk count
    max_chunks_per_iu.sort(reverse=True)
    print(f"\nTop 10 IUs by chunk count:")
    for num, cid in max_chunks_per_iu[:10]:
        print(f"  {cid:>12s}  {num:,} chunks")

    # Chunks exceeding max tokens
    over_max = sum(1 for t in token_counts if t > MAX_TOKENS)
    if over_max:
        over_2500 = sum(1 for t in token_counts if t > 2500)
        print(f"\nChunks > {MAX_TOKENS} tokens: {over_max:,}")
        print(f"Chunks > 2500 tokens: {over_2500:,}")


if __name__ == "__main__":
    main()
