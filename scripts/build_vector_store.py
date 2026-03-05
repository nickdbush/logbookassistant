"""Build Qdrant vector store and BM25 index from embeddings and chunks.

Creates:
- Qdrant collection via Docker (localhost:6333, gRPC on 6334)
- data/bm25_index.pkl (serialized BM25 index)
"""

import json
import pickle
import time
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    PointStruct,
    VectorParams,
)
from rank_bm25 import BM25Okapi

ROOT = Path(__file__).resolve().parent.parent

CHUNKS_PATH = ROOT / "data" / "corpus" / "chunks.parquet"
EMBEDDINGS_PATH = ROOT / "data" / "corpus" / "embeddings.npy"
CHUNK_IDS_PATH = ROOT / "data" / "corpus" / "chunk_ids.npy"
BM25_PATH = ROOT / "data" / "bm25_index.pkl"

EMBED_DIM = 1536
COLLECTION_NAME = "chunks"
UPLOAD_BATCH_SIZE = 10_000  # 10x larger batches
META_COLUMNS = [
    "canonical_iu_id", "content_type", "fault_codes",
    "part_numbers", "tool_references", "token_count",
]


def parse_list_field(val):
    """Parse stringified list fields like '["a", "b"]' → list."""
    if val is None or val == "" or val == "[]":
        return []
    try:
        return json.loads(val)
    except (json.JSONDecodeError, TypeError):
        return [val] if val else []


def build_qdrant(chunk_ids, embeddings, total):
    """Build Qdrant collection, reading metadata in batches from parquet."""
    print(f"\nBuilding Qdrant collection ({total:,} points)...")

    client = QdrantClient(
        host="localhost", port=6333, grpc_port=6334, prefer_grpc=True
    )

    # Recreate collection
    if client.collection_exists(COLLECTION_NAME):
        client.delete_collection(COLLECTION_NAME)

    client.create_collection(
        collection_name=COLLECTION_NAME,
        vectors_config=VectorParams(size=EMBED_DIM, distance=Distance.COSINE),
    )

    # Read metadata from parquet (no text column — saves ~1 GB)
    print("  Loading metadata columns...")
    meta_table = pq.read_table(CHUNKS_PATH, columns=META_COLUMNS)

    t0 = time.time()

    for i in range(0, total, UPLOAD_BATCH_SIZE):
        end = min(i + UPLOAD_BATCH_SIZE, total)

        # Bulk-convert embedding slice to list-of-lists (much faster than per-row)
        vectors = embeddings[i:end].tolist()

        # Slice metadata
        meta_slice = meta_table.slice(i, end - i)
        iu_ids = meta_slice.column("canonical_iu_id").to_pylist()
        ctypes = meta_slice.column("content_type").to_pylist()
        fcodes = meta_slice.column("fault_codes").to_pylist()
        pnums = meta_slice.column("part_numbers").to_pylist()
        trefs = meta_slice.column("tool_references").to_pylist()
        tcounts = meta_slice.column("token_count").to_pylist()

        points = [
            PointStruct(
                id=i + k,
                vector=vectors[k],
                payload={
                    "chunk_id": chunk_ids[i + k],
                    "canonical_iu_id": iu_ids[k],
                    "content_type": ctypes[k],
                    "fault_codes": parse_list_field(fcodes[k]),
                    "part_numbers": parse_list_field(pnums[k]),
                    "tool_references": parse_list_field(trefs[k]),
                    "token_count": tcounts[k],
                },
            )
            for k in range(end - i)
        ]
        client.upsert(collection_name=COLLECTION_NAME, points=points)

        elapsed = time.time() - t0
        rate = end / elapsed if elapsed > 0 else 0
        eta = (total - end) / rate if rate > 0 else 0
        print(f"  {end:,}/{total:,}  ({rate:,.0f} pts/s, ETA {eta / 60:.1f}m)")

    info = client.get_collection(COLLECTION_NAME)
    print(f"  Collection '{COLLECTION_NAME}': {info.points_count:,} points")
    client.close()


def build_bm25():
    """Build BM25 index, streaming text from parquet."""
    print("\nBuilding BM25 index...")

    texts = pq.read_table(CHUNKS_PATH, columns=["text"]).column("text").to_pylist()
    total = len(texts)
    print(f"  Tokenizing {total:,} documents...")

    tokenized = [doc.lower().split() for doc in texts]
    del texts  # free ~0.7 GB

    print("  Fitting BM25...")
    bm25 = BM25Okapi(tokenized)

    with open(BM25_PATH, "wb") as f:
        pickle.dump({"bm25": bm25, "num_docs": total}, f)

    size_mb = BM25_PATH.stat().st_size / (1024 * 1024)
    print(f"  BM25 index saved: {size_mb:.1f} MB")


def main():
    # Load chunk IDs
    print("Loading chunk IDs...")
    chunk_ids = np.load(CHUNK_IDS_PATH, allow_pickle=True).tolist()
    total = len(chunk_ids)

    # Load embeddings as memmap (no memory cost)
    print("Loading embeddings memmap...")
    embeddings = np.memmap(
        EMBEDDINGS_PATH, dtype=np.float32, mode="r", shape=(total, EMBED_DIM)
    )

    build_qdrant(chunk_ids, embeddings, total)
    del embeddings

    build_bm25()

    print("\nDone!")


if __name__ == "__main__":
    main()
