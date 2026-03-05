"""RAG assistant for CNH technician documentation.

Usage:
    .venv/bin/python scripts/assistant.py "query text" [--series SERIES] [--verbose]
"""

import argparse
import json
import pickle
import sys
import time
from pathlib import Path

import duckdb
import numpy as np
import pyarrow.parquet as pq
from dotenv import load_dotenv
from openai import OpenAI
from qdrant_client import QdrantClient

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

CHUNKS_PATH = ROOT / "data" / "corpus" / "chunks.parquet"
BM25_PATH = ROOT / "data" / "bm25_index.pkl"
DUCKDB_PATH = ROOT / "data" / "metadata.duckdb"
QDRANT_URL = "http://localhost:6333"
COLLECTION_NAME = "chunks"
EMBED_MODEL = "text-embedding-3-small"
DEFAULT_GEN_MODEL = "gpt-4o"

VECTOR_TOP_K = 20
VECTOR_TOP_K_SERIES = 200
BM25_TOP_K = 20
BM25_TOP_K_SERIES = 200
RRF_K = 60
FINAL_TOP_K = 15
TOKEN_BUDGET = 10_000


def parse_list_field(val):
    """Parse stringified list fields like '["a", "b"]' → list."""
    if val is None or val == "" or val == "[]":
        return []
    try:
        return json.loads(val)
    except (json.JSONDecodeError, TypeError):
        return [val] if val else []


def embed_query(client, text):
    resp = client.embeddings.create(model=EMBED_MODEL, input=[text])
    return resp.data[0].embedding


def expand_query(client, query):
    """Generate 3 alternative queries using gpt-4o-mini."""
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a search query expansion assistant for heavy equipment "
                    "technical documentation (CNH / Case / New Holland tractors, "
                    "combines, construction equipment). Given a technician's query, "
                    "generate exactly 3 alternative search queries, one per line. "
                    "Include synonyms, related technical terms, and fault code variants. "
                    "Output ONLY the 3 queries, no numbering or extra text."
                ),
            },
            {"role": "user", "content": query},
        ],
        temperature=0.7,
        max_tokens=200,
    )
    lines = [l.strip() for l in resp.choices[0].message.content.strip().split("\n") if l.strip()]
    return lines[:3]


def hybrid_search(queries, openai_client, qdrant, bm25, chunk_ids, series_filter=None, series_only=False):
    """Run vector + BM25 search for each query, merge with RRF."""
    # Widen retrieval window when filtering to a series
    vec_k = VECTOR_TOP_K_SERIES if series_filter else VECTOR_TOP_K
    bm25_k = BM25_TOP_K_SERIES if series_filter else BM25_TOP_K

    # Collect (chunk_id → list of ranks) across all query×method pairs
    all_ranks = {}  # chunk_id → list of (rank,)

    for query in queries:
        # Vector search
        query_vec = embed_query(openai_client, query)
        response = qdrant.query_points(
            collection_name=COLLECTION_NAME,
            query=query_vec,
            limit=vec_k,
        )
        for rank, hit in enumerate(response.points):
            cid = hit.payload["chunk_id"]
            all_ranks.setdefault(cid, []).append(rank)

        # BM25 search
        tokenized = query.lower().split()
        scores = bm25.get_scores(tokenized)
        top_indices = np.argsort(scores)[::-1][:bm25_k]
        for rank, idx in enumerate(top_indices):
            if scores[idx] <= 0:
                break
            cid = chunk_ids[idx]
            all_ranks.setdefault(cid, []).append(rank)

    # RRF scoring
    rrf_scores = {}
    for cid, ranks in all_ranks.items():
        rrf_scores[cid] = sum(1.0 / (RRF_K + r) for r in ranks)

    # Series boost or filter
    if series_filter:
        rrf_scores = apply_series_boost(rrf_scores, series_filter, series_only=series_only)

    # Sort by score descending
    ranked = sorted(rrf_scores.items(), key=lambda x: x[1], reverse=True)
    return ranked


def apply_series_boost(rrf_scores, series_filter, series_only=False, db_path=DUCKDB_PATH):
    """Boost chunks whose parent IU appears in the given series (3x), or filter to only those."""
    db = duckdb.connect(str(db_path), read_only=True)
    # Get all unique canonical_iu_ids we need to check
    # chunk_id format: {canonical_iu_id}_c{NNN}
    iu_ids = set()
    chunk_to_iu = {}
    for cid in rrf_scores:
        iu_id = cid.rsplit("_c", 1)[0]
        iu_ids.add(iu_id)
        chunk_to_iu[cid] = iu_id

    # Query appearances for these IUs
    if not iu_ids:
        db.close()
        return rrf_scores

    iu_list = list(iu_ids)
    placeholders = ",".join(["?"] * len(iu_list))
    rows = db.execute(
        f"SELECT canonical_id, appearances FROM canonical_ius WHERE canonical_id IN ({placeholders})",
        iu_list,
    ).fetchall()
    db.close()

    iu_series = {}
    for canonical_id, appearances in rows:
        apps = parse_list_field(appearances)
        series_set = {a.get("series", "") for a in apps if isinstance(a, dict)}
        iu_series[canonical_id] = series_set

    boosted = {}
    for cid, score in rrf_scores.items():
        iu_id = chunk_to_iu[cid]
        in_series = series_filter in iu_series.get(iu_id, set())
        if series_only and not in_series:
            continue
        boosted[cid] = score * 3.0 if in_series else score

    return boosted


def assemble_context(ranked_chunks, chunk_ids, texts, content_types, token_counts, id_to_idx, num_chunks_arr, chunk_indices_arr):
    """Assemble context from top chunks, respecting token budget."""
    db = duckdb.connect(str(DUCKDB_PATH), read_only=True)

    context_blocks = []
    sources = []
    total_tokens = 0

    for cid, rrf_score in ranked_chunks[:FINAL_TOP_K]:
        idx = id_to_idx.get(cid)
        if idx is None:
            continue

        chunk_text = texts[idx]
        tokens = token_counts[idx]

        if total_tokens + tokens > TOKEN_BUDGET:
            break

        # Get parent IU metadata
        iu_id = cid.rsplit("_c", 1)[0]
        iu_row = db.execute(
            "SELECT content_type, fault_codes, iu_cross_references, appearances FROM canonical_ius WHERE canonical_id = ?",
            [iu_id],
        ).fetchone()

        series_list = ""
        content_type = content_types[idx] or "unknown"
        if iu_row:
            content_type = iu_row[0] or content_type
            apps = parse_list_field(iu_row[3])
            series_list = ", ".join(sorted({a["series"] for a in apps if isinstance(a, dict) and "series" in a}))

        # Pull adjacent chunks if multi-chunk IU
        full_text = chunk_text
        nc = num_chunks_arr[idx]
        ci = chunk_indices_arr[idx]
        if nc is not None and nc > 1 and ci is not None:
            # Look for adjacent chunks
            for delta in [-1, 1]:
                adj_ci = ci + delta
                if 0 <= adj_ci < nc:
                    adj_cid = f"{iu_id}_c{adj_ci:03d}"
                    adj_idx = id_to_idx.get(adj_cid)
                    if adj_idx is not None:
                        adj_tokens = token_counts[adj_idx]
                        if total_tokens + tokens + adj_tokens <= TOKEN_BUDGET:
                            if delta == -1:
                                full_text = texts[adj_idx] + "\n\n" + full_text
                            else:
                                full_text = full_text + "\n\n" + texts[adj_idx]
                            tokens += adj_tokens

        total_tokens += tokens

        block = f"[Source: {iu_id} | Type: {content_type} | Series: {series_list}]\n{full_text}"
        context_blocks.append(block)

        sources.append({
            "iu_id": iu_id,
            "content_type": content_type,
            "series": series_list,
            "rrf_score": rrf_score,
        })

    db.close()
    return "\n\n---\n\n".join(context_blocks), sources


SYSTEM_PROMPT = """\
You are a CNH technical assistant helping equipment dealers diagnose and repair \
CNH (Case, New Holland, STEYR) agricultural and construction equipment.

You have access to official service documentation including diagnostic procedures, \
fault codes, repair steps, specification tables, wiring descriptions, and parts info.

Guidelines:
- Provide structured, step-by-step diagnostic and repair guidance
- Always cite source IU IDs when referencing specific procedures (e.g., [Source: 12345678])
- Include relevant fault codes, specifications, and torque values when available
- If the documentation doesn't contain enough info, say so clearly
- Use clear technical language appropriate for dealer technicians
- When multiple procedures apply, list them in logical diagnostic order
"""


def generate_answer(client, query, context, sources, model=DEFAULT_GEN_MODEL):
    """Generate answer from context using the specified model."""
    user_msg = f"""Question: {query}

Reference Documentation:
{context}

Please answer the question using the documentation above. Cite sources using [Source: IU_ID] format."""

    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_msg},
        ],
        temperature=0.2,
        max_completion_tokens=2000,
    )
    return resp.choices[0].message.content


def main():
    parser = argparse.ArgumentParser(description="CNH Technician RAG Assistant")
    parser.add_argument("query", help="Technical question to answer")
    parser.add_argument("--series", help="Series to prioritize (e.g., 'A.A.01.034')")
    parser.add_argument("--series-only", action="store_true", help="Only return results from the specified series")
    parser.add_argument("--model", default=DEFAULT_GEN_MODEL, help=f"Generation model (default: {DEFAULT_GEN_MODEL})")
    parser.add_argument("--verbose", action="store_true", help="Show retrieval debug info")
    args = parser.parse_args()

    t0 = time.time()

    # --- Startup ---
    print("Loading resources...", file=sys.stderr)

    openai_client = OpenAI()
    qdrant = QdrantClient(url=QDRANT_URL)

    print("  BM25 index...", file=sys.stderr)
    with open(BM25_PATH, "rb") as f:
        bm25_data = pickle.load(f)
    bm25 = bm25_data["bm25"]

    print("  Chunks parquet...", file=sys.stderr)
    chunks_table = pq.read_table(
        CHUNKS_PATH,
        columns=["chunk_id", "text", "content_type", "token_count", "num_chunks", "chunk_index"],
    )
    chunk_ids = chunks_table.column("chunk_id").to_pylist()
    texts = chunks_table.column("text").to_pylist()
    content_types = chunks_table.column("content_type").to_pylist()
    token_counts = chunks_table.column("token_count").to_pylist()
    num_chunks_arr = chunks_table.column("num_chunks").to_pylist()
    chunk_indices_arr = chunks_table.column("chunk_index").to_pylist()

    id_to_idx = {cid: i for i, cid in enumerate(chunk_ids)}

    load_time = time.time() - t0
    print(f"  Loaded in {load_time:.1f}s", file=sys.stderr)

    # --- Step 1: Query Expansion ---
    print("Expanding query...", file=sys.stderr)
    expansions = expand_query(openai_client, args.query)
    all_queries = [args.query] + expansions

    if args.verbose:
        print(f"\n{'='*80}")
        print("QUERY EXPANSIONS:")
        for i, q in enumerate(all_queries):
            print(f"  {i}. {q}")

    # --- Step 2: Hybrid Retrieval ---
    print("Retrieving...", file=sys.stderr)
    ranked = hybrid_search(all_queries, openai_client, qdrant, bm25, chunk_ids, args.series, args.series_only)

    if args.verbose:
        print(f"\n{'='*80}")
        print(f"TOP {min(30, len(ranked))} RETRIEVED CHUNKS (RRF scores):")
        for i, (cid, score) in enumerate(ranked[:30]):
            idx = id_to_idx.get(cid, -1)
            ct = content_types[idx] if idx >= 0 else "?"
            snippet = texts[idx][:100] if idx >= 0 else ""
            print(f"  {i+1:2d}. [{score:.4f}] {cid}  type={ct}")
            print(f"      {snippet}")

    # --- Step 3: Context Assembly ---
    print("Assembling context...", file=sys.stderr)
    context, sources = assemble_context(
        ranked, chunk_ids, texts, content_types, token_counts,
        id_to_idx, num_chunks_arr, chunk_indices_arr,
    )

    # --- Step 4: Generation ---
    print(f"Generating answer ({args.model})...", file=sys.stderr)
    answer = generate_answer(openai_client, args.query, context, sources, model=args.model)

    # --- Step 5: Output ---
    qdrant.close()

    print(f"\n{'='*80}")
    print("ANSWER")
    print(f"{'='*80}\n")
    print(answer)

    print(f"\n{'='*80}")
    print("SOURCES")
    print(f"{'='*80}")
    for s in sources:
        print(f"  IU {s['iu_id']}  |  {s['content_type']}")
        if s['series']:
            print(f"    Series: {s['series']}")

    total_time = time.time() - t0
    print(f"\n[Completed in {total_time:.1f}s]", file=sys.stderr)


if __name__ == "__main__":
    main()
