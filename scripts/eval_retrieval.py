"""Evaluate retrieval quality using LLM-as-judge.

Runs hybrid retrieval for a query, then asks an LLM to rate each
retrieved chunk as HIGH / MEDIUM / LOW relevance.

Usage:
    .venv/bin/python scripts/eval_retrieval.py "How to replace DPF filter on T7.270?"
    .venv/bin/python scripts/eval_retrieval.py "fault code 47623" --series A.A.01.034
    .venv/bin/python scripts/eval_retrieval.py queries.txt  # one query per line, batch mode
"""

import argparse
import json
import pickle
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
from dotenv import load_dotenv
from openai import OpenAI
from qdrant_client import QdrantClient

sys.path.insert(0, str(Path(__file__).resolve().parent))
from assistant import (
    CHUNKS_PATH, BM25_PATH, QDRANT_URL, COLLECTION_NAME, EMBED_MODEL,
    RRF_K, FINAL_TOP_K,
    expand_query, hybrid_search, parse_list_field,
)

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

JUDGE_MODEL = "gpt-4o-mini"
EVAL_TOP_K = 15
RESULTS_DIR = ROOT / "data" / "eval"


def load_resources():
    """Load BM25, chunks, Qdrant — same as assistant.py."""
    t0 = time.time()
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
    id_to_idx = {cid: i for i, cid in enumerate(chunk_ids)}

    print(f"  Loaded in {time.time() - t0:.1f}s", file=sys.stderr)
    return {
        "openai": openai_client, "qdrant": qdrant, "bm25": bm25,
        "chunk_ids": chunk_ids, "texts": texts, "content_types": content_types,
        "token_counts": token_counts, "id_to_idx": id_to_idx,
    }


def judge_relevance(openai_client, query, chunks):
    """Ask LLM to rate each chunk's relevance to the query.

    Returns list of dicts: [{rating, reasoning}, ...]
    """
    chunk_block = []
    for i, c in enumerate(chunks):
        # Truncate long chunks to ~800 chars for the judge
        text = c["text"][:800]
        if len(c["text"]) > 800:
            text += " [...]"
        chunk_block.append(f"--- CHUNK {i} ---\nID: {c['chunk_id']}\nType: {c['content_type']}\n{text}")

    chunks_text = "\n\n".join(chunk_block)

    resp = openai_client.chat.completions.create(
        model=JUDGE_MODEL,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are evaluating search result relevance for a heavy equipment "
                    "technical documentation system (CNH / Case / New Holland). "
                    "A technician has asked a question, and the system retrieved document chunks.\n\n"
                    "For EACH chunk, rate its relevance to the query:\n"
                    "- HIGH: Directly answers or contains key information for the query\n"
                    "- MEDIUM: Related topic but doesn't directly answer; could be useful context\n"
                    "- LOW: Not relevant to the query\n\n"
                    "Respond with a JSON array, one object per chunk, in order:\n"
                    '[{"chunk": 0, "rating": "HIGH", "reason": "brief explanation"}, ...]\n\n'
                    "Output ONLY the JSON array."
                ),
            },
            {
                "role": "user",
                "content": f"QUERY: {query}\n\nRETRIEVED CHUNKS:\n\n{chunks_text}",
            },
        ],
        temperature=0,
        max_tokens=2000,
        response_format={"type": "json_object"},
    )

    raw = resp.choices[0].message.content
    parsed = json.loads(raw)
    # Handle both {"ratings": [...]} and [...] formats
    if isinstance(parsed, dict):
        for key in ("ratings", "results", "chunks"):
            if key in parsed:
                parsed = parsed[key]
                break
        else:
            parsed = list(parsed.values())[0] if parsed else []
    return parsed


def evaluate_query(query, resources, series=None, top_k=EVAL_TOP_K):
    """Run retrieval + LLM judgment for a single query."""
    openai_client = resources["openai"]
    id_to_idx = resources["id_to_idx"]
    texts = resources["texts"]
    content_types = resources["content_types"]
    token_counts = resources["token_counts"]

    # Retrieve
    expansions = expand_query(openai_client, query)
    all_queries = [query] + expansions

    ranked = hybrid_search(
        all_queries, openai_client, resources["qdrant"],
        resources["bm25"], resources["chunk_ids"],
        series_filter=series,
    )

    # Build chunk list for judging
    chunks = []
    for cid, rrf_score in ranked[:top_k]:
        idx = id_to_idx.get(cid)
        if idx is None:
            continue
        chunks.append({
            "chunk_id": cid,
            "text": texts[idx],
            "content_type": content_types[idx] or "unknown",
            "token_count": token_counts[idx],
            "rrf_score": rrf_score,
        })

    # Judge
    ratings = judge_relevance(openai_client, query, chunks)

    # Merge ratings into chunks
    for i, c in enumerate(chunks):
        if i < len(ratings):
            r = ratings[i]
            c["rating"] = r.get("rating", "?")
            c["reason"] = r.get("reason", "")
        else:
            c["rating"] = "?"
            c["reason"] = ""

    return {"query": query, "expansions": expansions, "series": series, "chunks": chunks}


def print_results(result):
    """Print a formatted table of results."""
    query = result["query"]
    series = result["series"]

    print(f"\n{'='*90}")
    print(f"QUERY: {query}")
    if series:
        print(f"SERIES: {series}")
    print(f"EXPANSIONS: {', '.join(result['expansions'])}")
    print(f"{'='*90}")

    rating_colors = {"HIGH": "\033[32m", "MEDIUM": "\033[33m", "LOW": "\033[31m"}
    reset = "\033[0m"

    counts = {"HIGH": 0, "MEDIUM": 0, "LOW": 0}
    print(f"\n{'#':>3}  {'Rating':<8} {'Score':>7}  {'Type':<22} {'Chunk ID'}")
    print(f"{'':>3}  {'Reason'}")
    print(f"{'-'*90}")

    for i, c in enumerate(result["chunks"]):
        rating = c["rating"]
        counts[rating] = counts.get(rating, 0) + 1
        color = rating_colors.get(rating, "")
        title_line = c["text"].split("\n")[0][:60]

        print(f"{i+1:3d}  {color}{rating:<8}{reset} {c['rrf_score']:7.4f}  {c['content_type']:<22} {c['chunk_id']}")
        print(f"     {c['reason']}")
        print(f"     {title_line}")
        print()

    # Summary
    total = len(result["chunks"])
    high = counts.get("HIGH", 0)
    med = counts.get("MEDIUM", 0)
    low = counts.get("LOW", 0)
    precision_at_k = high / total if total else 0

    print(f"{'='*90}")
    print(f"SUMMARY: {high} HIGH / {med} MEDIUM / {low} LOW  (precision@{total}: {precision_at_k:.0%})")
    print(f"{'='*90}")

    return {"high": high, "medium": med, "low": low, "total": total}


def save_result(result):
    """Append result to JSONL eval log."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    outfile = RESULTS_DIR / "retrieval_evals.jsonl"
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        **result,
    }
    with open(outfile, "a") as f:
        f.write(json.dumps(record) + "\n")
    print(f"\nSaved to {outfile}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description="Evaluate retrieval quality with LLM-as-judge")
    parser.add_argument("query", help="Query string, or path to .txt file with one query per line")
    parser.add_argument("--series", help="Series filter")
    parser.add_argument("--top-k", type=int, default=EVAL_TOP_K, help=f"Number of chunks to evaluate (default: {EVAL_TOP_K})")
    parser.add_argument("--no-save", action="store_true", help="Don't save results to eval log")
    args = parser.parse_args()

    resources = load_resources()

    # Check if query is a file path
    query_path = Path(args.query)
    if query_path.exists() and query_path.suffix == ".txt":
        queries = [line.strip() for line in query_path.read_text().splitlines() if line.strip()]
    else:
        queries = [args.query]

    all_stats = []
    for query in queries:
        print(f"\nEvaluating: {query}", file=sys.stderr)
        result = evaluate_query(query, resources, series=args.series, top_k=args.top_k)
        stats = print_results(result)
        all_stats.append(stats)
        if not args.no_save:
            save_result(result)

    # Batch summary
    if len(queries) > 1:
        total_h = sum(s["high"] for s in all_stats)
        total_m = sum(s["medium"] for s in all_stats)
        total_l = sum(s["low"] for s in all_stats)
        total_n = sum(s["total"] for s in all_stats)
        print(f"\n{'='*90}")
        print(f"BATCH SUMMARY ({len(queries)} queries):")
        print(f"  {total_h} HIGH / {total_m} MEDIUM / {total_l} LOW")
        print(f"  Overall precision: {total_h/total_n:.0%}")
        print(f"{'='*90}")

    resources["qdrant"].close()


if __name__ == "__main__":
    main()
