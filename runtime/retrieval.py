"""Hybrid search: vector (Qdrant) + BM25, merged with RRF."""

from __future__ import annotations

import asyncio
import json
import time

import numpy as np

from generation import estimate_cost

EMBED_MODEL = "text-embedding-3-small"
COLLECTION_NAME = "chunks"

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
    db=None,
    iu_series_map=None,
    iu_to_chunk_indices=None,
):
    """Run vector + BM25 search for each query, merge with RRF."""
    widen = series_filter or tt_id
    vec_k = VECTOR_TOP_K_SERIES if widen else VECTOR_TOP_K
    bm25_k = BM25_TOP_K_SERIES if widen else BM25_TOP_K

    all_ranks: dict[str, list[int]] = {}
    spans = []
    model_calls = []

    # Build BM25 hard mask for _only modes (zero out non-matching before top-k)
    # Boosting happens only at the RRF level to avoid double-boosting
    bm25_mask = None
    applicable_miuids = None
    n_chunks = len(chunk_ids)

    if tt_id and iu_to_chunk_indices:
        cur = db.cursor() if db else None
        if cur:
            rows = cur.execute(
                "SELECT iu_miuid FROM iu_tt_applicability WHERE tt_id = ?", [tt_id]
            ).fetchall()
            cur.close()
            applicable_miuids = {r[0] for r in rows}
            if applicable_miuids and tt_only:
                tt_mask = np.zeros(n_chunks, dtype=bool)
                for miuid in applicable_miuids:
                    for idx in iu_to_chunk_indices.get(miuid, []):
                        tt_mask[idx] = True
                bm25_mask = tt_mask

    if series_only and series_filter and iu_series_map and iu_to_chunk_indices:
        series_mask = np.zeros(n_chunks, dtype=bool)
        for iu_id, series_set in iu_series_map.items():
            if series_filter in series_set:
                base_miuid = iu_id
                if base_miuid.endswith(("_v1", "_v2", "_v3", "_v4", "_v5")):
                    base_miuid = base_miuid.rsplit("_v", 1)[0]
                for idx in iu_to_chunk_indices.get(base_miuid, []):
                    series_mask[idx] = True
                for idx in iu_to_chunk_indices.get(iu_id, []):
                    series_mask[idx] = True
        if bm25_mask is not None:
            bm25_mask = bm25_mask & series_mask
        else:
            bm25_mask = series_mask

    # Step 5: Batch all embeddings in a single API call
    t = time.time()
    embed_resp = await openai_client.embeddings.create(model=EMBED_MODEL, input=queries)
    total_embed_tokens = embed_resp.usage.prompt_tokens
    query_vecs = [item.embedding for item in embed_resp.data]
    embed_ms = int((time.time() - t) * 1000)
    spans.append({"name": "embed_batch", "duration_ms": embed_ms})
    model_calls.append({
        "name": "embed_batch",
        "model": EMBED_MODEL,
        "input_tokens": total_embed_tokens,
        "output_tokens": 0,
        "cost_usd": estimate_cost(EMBED_MODEL, total_embed_tokens, 0),
    })

    # Run all qdrant searches + all BM25 searches in parallel
    def _qdrant_search(query_vec, k):
        response = qdrant.query_points(
            collection_name=COLLECTION_NAME,
            query=query_vec,
            limit=k,
        )
        return [(rank, hit.payload["chunk_id"]) for rank, hit in enumerate(response.points)]

    def _bm25_search(query_text):
        tokenized = query_text.lower().split()
        scores = bm25.get_scores(tokenized)

        # Apply hard mask for _only modes (Step 6)
        if bm25_mask is not None:
            scores[~bm25_mask] = 0

        # Step 3: argpartition instead of full argsort
        nonzero_count = np.count_nonzero(scores)
        k = min(bm25_k, nonzero_count)
        if k == 0:
            return []
        top_unsorted = np.argpartition(scores, -k)[-k:]
        top_indices = top_unsorted[np.argsort(scores[top_unsorted])[::-1]]

        results = []
        for rank_i, idx in enumerate(top_indices):
            if scores[idx] <= 0:
                break
            results.append((rank_i, chunk_ids[idx]))
        return results

    t = time.time()
    # Build all coroutines for parallel execution
    qdrant_coros = [asyncio.to_thread(_qdrant_search, qv, vec_k) for qv in query_vecs]
    bm25_coros = [asyncio.to_thread(_bm25_search, q) for q in queries]
    all_results = await asyncio.gather(*qdrant_coros, *bm25_coros)
    search_ms = int((time.time() - t) * 1000)
    spans.append({"name": "parallel_search", "duration_ms": search_ms})

    n_queries = len(queries)
    qdrant_results = all_results[:n_queries]
    bm25_results_list = all_results[n_queries:]

    for qdrant_hits in qdrant_results:
        for rank, cid in qdrant_hits:
            all_ranks.setdefault(cid, []).append(rank)
    for bm25_hits in bm25_results_list:
        for rank, cid in bm25_hits:
            all_ranks.setdefault(cid, []).append(rank)

    # RRF scoring
    t = time.time()
    rrf_scores = {}
    for cid, ranks in all_ranks.items():
        rrf_scores[cid] = sum(1.0 / (RRF_K + r) for r in ranks)
    spans.append({"name": "rrf_ranking", "duration_ms": int((time.time() - t) * 1000)})

    # Series boost/filter on RRF results (using cached iu_series_map)
    if series_filter and iu_series_map:
        t = time.time()
        rrf_scores = _apply_series_boost_cached(rrf_scores, series_filter, iu_series_map, series_only=series_only)
        spans.append({"name": "series_boost", "duration_ms": int((time.time() - t) * 1000)})

    # TT boost/filter on RRF results (reuse applicable_miuids if already fetched)
    if tt_id:
        t = time.time()
        rrf_scores = apply_tt_filter(rrf_scores, tt_id, tt_only=tt_only, db=db, applicable_miuids=applicable_miuids)
        spans.append({"name": "tt_boost", "duration_ms": int((time.time() - t) * 1000)})

    ranked = sorted(rrf_scores.items(), key=lambda x: x[1], reverse=True)
    return ranked, spans, model_calls, applicable_miuids


def _apply_series_boost_cached(rrf_scores, series_filter, iu_series_map, series_only=False):
    """Boost/filter RRF scores using pre-cached iu_series_map (no DuckDB)."""
    boosted = {}
    for cid, score in rrf_scores.items():
        iu_id = cid.rsplit("_c", 1)[0]
        in_series = series_filter in iu_series_map.get(iu_id, frozenset())
        if series_only and not in_series:
            continue
        boosted[cid] = score * 3.0 if in_series else score
    return boosted


def apply_tt_filter(rrf_scores, tt_id, tt_only=False, db=None, applicable_miuids=None):
    """Boost/filter chunks whose parent IU applies to the given technical type."""
    if applicable_miuids is None and db is not None:
        cur = db.cursor()
        rows = cur.execute(
            "SELECT iu_miuid FROM iu_tt_applicability WHERE tt_id = ?", [tt_id]
        ).fetchall()
        cur.close()
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


def assemble_context(
    ranked_chunks, chunk_ids, texts, content_types, token_counts, id_to_idx,
    num_chunks_arr, chunk_indices_arr, series_filter=None,
    iu_series_map=None, db=None, applicable_miuids=None,
):
    """Assemble context from top chunks, respecting token budget."""
    # Batch-fetch metadata for the small number of IUs we'll actually use
    candidate_iu_ids = []
    for cid, _ in ranked_chunks[:FINAL_TOP_K]:
        if id_to_idx.get(cid) is not None:
            candidate_iu_ids.append(cid.rsplit("_c", 1)[0])
    iu_metadata_local = {}
    if candidate_iu_ids and db is not None:
        unique_ids = list(set(candidate_iu_ids))
        placeholders = ",".join(["?"] * len(unique_ids))
        cur = db.cursor()
        rows = cur.execute(
            f"SELECT canonical_id, content_type, title FROM canonical_ius WHERE canonical_id IN ({placeholders})",
            unique_ids,
        ).fetchall()
        cur.close()
        for row in rows:
            iu_metadata_local[row[0]] = (row[1], row[2])

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

        in_target_series = False
        content_type = content_types[idx] or "unknown"
        title = ""
        iu_row = iu_metadata_local.get(iu_id)
        if iu_row:
            content_type = iu_row[0] or content_type
            title = iu_row[1] or ""
        if applicable_miuids:
            base_miuid = iu_id
            if base_miuid.endswith(("_v1", "_v2", "_v3", "_v4", "_v5")):
                base_miuid = base_miuid.rsplit("_v", 1)[0]
            in_target_series = base_miuid in applicable_miuids
        elif series_filter and iu_series_map:
            in_target_series = series_filter in iu_series_map.get(iu_id, frozenset())

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

        block = f"[Source: {iu_id} | {title} | Type: {content_type}]\n{full_text}"
        context_blocks.append(block)

        sources.append({
            "iu_id": iu_id,
            "title": title,
            "content_type": content_type,
            "in_target_series": in_target_series,
            "rrf_score": rrf_score,
        })

    return "\n\n---\n\n".join(context_blocks), sources
