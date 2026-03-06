"""Hybrid search: vector (Qdrant) + BM25, merged with RRF."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import duckdb
import numpy as np

EMBED_MODEL = "text-embedding-3-small"
COLLECTION_NAME = "chunks"

VECTOR_TOP_K = 20
VECTOR_TOP_K_SERIES = 200
BM25_TOP_K = 20
BM25_TOP_K_SERIES = 200
RRF_K = 60
FINAL_TOP_K = 15
TOKEN_BUDGET = 10_000


def _data_dir() -> Path:
    return Path(os.environ.get("DATA_DIR", "/app/data"))


def parse_list_field(val):
    """Parse stringified list fields like '["a", "b"]' → list."""
    if val is None or val == "" or val == "[]":
        return []
    try:
        return json.loads(val)
    except (json.JSONDecodeError, TypeError):
        return [val] if val else []


async def embed_query(client, text: str) -> list[float]:
    resp = await client.embeddings.create(model=EMBED_MODEL, input=[text])
    return resp.data[0].embedding


async def hybrid_search(
    queries,
    openai_client,
    qdrant,
    bm25,
    chunk_ids,
    series_filter=None,
    series_only=False,
    tt_id=None,
    tt_only=False,
):
    """Run vector + BM25 search for each query, merge with RRF."""
    widen = series_filter or tt_id
    vec_k = VECTOR_TOP_K_SERIES if widen else VECTOR_TOP_K
    bm25_k = BM25_TOP_K_SERIES if widen else BM25_TOP_K

    all_ranks: dict[str, list[int]] = {}

    for query in queries:
        # Vector search (async embed, sync qdrant query via thread)
        query_vec = await embed_query(openai_client, query)
        response = await asyncio.to_thread(
            qdrant.query_points,
            collection_name=COLLECTION_NAME,
            query=query_vec,
            limit=vec_k,
        )
        for rank, hit in enumerate(response.points):
            cid = hit.payload["chunk_id"]
            all_ranks.setdefault(cid, []).append(rank)

        # BM25 search (CPU-bound, run in thread)
        def _bm25_search(q=query):
            tokenized = q.lower().split()
            scores = bm25.get_scores(tokenized)
            top_indices = np.argsort(scores)[::-1][:bm25_k]
            results = []
            for rank_i, idx in enumerate(top_indices):
                if scores[idx] <= 0:
                    break
                results.append((rank_i, chunk_ids[idx]))
            return results

        bm25_results = await asyncio.to_thread(_bm25_search)
        for rank, cid in bm25_results:
            all_ranks.setdefault(cid, []).append(rank)

    # RRF scoring
    rrf_scores = {}
    for cid, ranks in all_ranks.items():
        rrf_scores[cid] = sum(1.0 / (RRF_K + r) for r in ranks)

    # Series boost or filter
    if series_filter:
        rrf_scores = apply_series_boost(rrf_scores, series_filter, series_only=series_only)

    # TT boost or filter
    if tt_id:
        rrf_scores = apply_tt_filter(rrf_scores, tt_id, tt_only=tt_only)

    ranked = sorted(rrf_scores.items(), key=lambda x: x[1], reverse=True)
    return ranked


def apply_series_boost(rrf_scores, series_filter, series_only=False):
    """Boost chunks whose parent IU appears in the given series (3x), or filter to only those."""
    db_path = _data_dir() / "metadata.duckdb"
    db = duckdb.connect(str(db_path), read_only=True)

    iu_ids = set()
    chunk_to_iu = {}
    for cid in rrf_scores:
        iu_id = cid.rsplit("_c", 1)[0]
        iu_ids.add(iu_id)
        chunk_to_iu[cid] = iu_id

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


def apply_tt_filter(rrf_scores, tt_id, tt_only=False):
    """Boost/filter chunks whose parent IU applies to the given technical type."""
    db_path = _data_dir() / "metadata.duckdb"
    db = duckdb.connect(str(db_path), read_only=True)

    rows = db.execute(
        "SELECT iu_miuid FROM iu_tt_applicability WHERE tt_id = ?", [tt_id]
    ).fetchall()
    db.close()

    applicable_miuids = {r[0] for r in rows}
    if not applicable_miuids:
        return rrf_scores

    boosted = {}
    for cid, score in rrf_scores.items():
        iu_id = cid.rsplit("_c", 1)[0]
        base_miuid = iu_id
        if base_miuid.endswith(("_v1", "_v2", "_v3", "_v4", "_v5")):
            base_miuid = base_miuid.rsplit("_v", 1)[0]

        in_tt = base_miuid in applicable_miuids
        if tt_only and not in_tt:
            continue
        boosted[cid] = score * 3.0 if in_tt else score

    return boosted


def assemble_context(ranked_chunks, chunk_ids, texts, content_types, token_counts, id_to_idx, num_chunks_arr, chunk_indices_arr):
    """Assemble context from top chunks, respecting token budget."""
    db_path = _data_dir() / "metadata.duckdb"
    db = duckdb.connect(str(db_path), read_only=True)

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
