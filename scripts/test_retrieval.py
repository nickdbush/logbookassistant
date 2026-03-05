"""Sanity-check retrieval: run 5 test queries against Qdrant and BM25.

Shows top-5 results from each, with chunk_id, score, content_type,
and first 200 chars of text.
"""

import pickle
import sys
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
from dotenv import load_dotenv
from openai import OpenAI
from qdrant_client import QdrantClient

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

CHUNKS_PATH = ROOT / "data" / "corpus" / "chunks.parquet"
QDRANT_URL = "http://localhost:6333"
BM25_PATH = ROOT / "data" / "bm25_index.pkl"
CHUNK_IDS_PATH = ROOT / "data" / "corpus" / "chunk_ids.npy"

COLLECTION_NAME = "chunks"
TOP_K = 5
EMBED_MODEL = "text-embedding-3-small"

TEST_QUERIES = [
    "How to diagnose fault code 523774",
    "hydraulic pump pressure test procedure",
    "engine oil specification and capacity",
    "wiring harness connector pin layout",
    "SCR aftertreatment DPF regeneration",
]


def embed_query(client, text):
    resp = client.embeddings.create(model=EMBED_MODEL, input=[text])
    return resp.data[0].embedding


def main():
    openai_client = OpenAI()
    qdrant = QdrantClient(url=QDRANT_URL)

    # Load BM25
    print("Loading BM25 index...")
    with open(BM25_PATH, "rb") as f:
        bm25_data = pickle.load(f)
    bm25 = bm25_data["bm25"]

    # Load chunk texts and IDs for BM25 lookup
    print("Loading chunks...")
    chunks_table = pq.read_table(CHUNKS_PATH, columns=["chunk_id", "text", "content_type"])
    chunk_ids = chunks_table.column("chunk_id").to_pylist()
    texts = chunks_table.column("text").to_pylist()
    content_types = chunks_table.column("content_type").to_pylist()

    # Build chunk_id → index map for text lookup from Qdrant results
    id_to_idx = {cid: i for i, cid in enumerate(chunk_ids)}

    for query in TEST_QUERIES:
        print(f"\n{'='*80}")
        print(f"QUERY: {query}")
        print(f"{'='*80}")

        # --- Vector search ---
        print(f"\n  VECTOR (top {TOP_K}):")
        query_vec = embed_query(openai_client, query)
        response = qdrant.query_points(
            collection_name=COLLECTION_NAME,
            query=query_vec,
            limit=TOP_K,
        )
        for rank, hit in enumerate(response.points, 1):
            cid = hit.payload["chunk_id"]
            ct = hit.payload.get("content_type", "?")
            idx = id_to_idx.get(cid, -1)
            snippet = texts[idx][:200] if idx >= 0 else "(text not found)"
            print(f"    {rank}. [{hit.score:.4f}] {cid}  type={ct}")
            print(f"       {snippet}")

        # --- BM25 search ---
        print(f"\n  BM25 (top {TOP_K}):")
        tokenized_query = query.lower().split()
        scores = bm25.get_scores(tokenized_query)
        top_indices = np.argsort(scores)[::-1][:TOP_K]
        for rank, idx in enumerate(top_indices, 1):
            cid = chunk_ids[idx]
            ct = content_types[idx]
            snippet = texts[idx][:200]
            print(f"    {rank}. [{scores[idx]:.4f}] {cid}  type={ct}")
            print(f"       {snippet}")

    qdrant.close()
    print(f"\n{'='*80}")
    print("Sanity check complete.")


if __name__ == "__main__":
    main()
