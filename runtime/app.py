"""FastAPI app for CNH technician RAG assistant."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import pickle
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal

import asyncpg
import duckdb
import numpy as np
import pyarrow.parquet as pq
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from anthropic import AsyncAnthropic
from openai import AsyncOpenAI
from pydantic import BaseModel
from qdrant_client import QdrantClient

from generation import expand_query, generate_answer
from retrieval import assemble_context, hybrid_search
from vin import resolve_identifier

load_dotenv()
logger = logging.getLogger("assistant")

DATA_DIR = Path(os.environ.get("DATA_DIR", "/app/data"))
QDRANT_URL = os.environ.get("QDRANT_URL", "http://localhost:6333")
DATABASE_URL = os.environ.get("DATABASE_URL", "")


# --- Pydantic models ---

# Outbound: server → client
class SelectOption(BaseModel):
    label: str
    value: str


class Question(BaseModel):
    id: str
    type: Literal["single_select", "multi_select", "number"]
    text: str
    options: list[SelectOption] | None = None
    min: float | None = None
    max: float | None = None
    step: float | None = None
    unit: str | None = None


# Inbound: client → server
class QuestionAnswer(BaseModel):
    question_id: str
    selected: list[str] | None = None
    number: float | None = None


class ConversationTurn(BaseModel):
    role: Literal["assistant", "user"]
    text: str | None = None
    questions: list[Question] | None = None   # assistant turns only
    answers: list[QuestionAnswer] | None = None  # user turns only


class QueryRequest(BaseModel):
    query: str
    series: str | None = None
    series_only: bool = False
    identifier: str | None = None
    vin: str | None = None  # backward compat alias for identifier
    vin_only: bool = False
    model: str = "claude-sonnet-4-6"
    conversation: list[ConversationTurn] | None = None
    sources: list[dict] | None = None
    conversation_id: str | None = None
    expanded_queries: list[str] | None = None


class Span(BaseModel):
    name: str
    duration_ms: int


class ModelCall(BaseModel):
    name: str
    model: str
    input_tokens: int
    output_tokens: int
    cost_usd: float


class QueryResponse(BaseModel):
    answer: str
    questions: list[Question] | None = None
    sources: list[dict]
    vin_info: dict | None = None
    timing: dict
    spans: list[Span]
    model_calls: list[ModelCall]
    conversation_id: str
    expanded_queries: list[str]


# --- Lifespan ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Loading resources...")

    app.state.openai = AsyncOpenAI()
    app.state.anthropic = AsyncAnthropic()
    app.state.qdrant = QdrantClient(url=QDRANT_URL)

    # Shared read-only DuckDB connection
    db_path = DATA_DIR / "metadata.duckdb"
    app.state.duckdb = duckdb.connect(str(db_path), read_only=True)
    logger.info("  DuckDB: %s", db_path)

    # Postgres connection pool for token validation
    if DATABASE_URL:
        app.state.pg_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    else:
        app.state.pg_pool = None

    # BM25 index
    bm25_path = DATA_DIR / "bm25_index.pkl"
    logger.info("  BM25 index: %s", bm25_path)
    with open(bm25_path, "rb") as f:
        bm25_data = pickle.load(f)
    app.state.bm25 = bm25_data["bm25"]

    # Chunks parquet
    chunks_path = DATA_DIR / "corpus" / "chunks.parquet"
    logger.info("  Chunks: %s", chunks_path)
    chunks_table = pq.read_table(
        chunks_path,
        columns=["chunk_id", "text", "content_type", "token_count", "num_chunks", "chunk_index"],
    )
    app.state.chunk_ids = chunks_table.column("chunk_id").to_pylist()
    app.state.texts = chunks_table.column("text").to_pylist()
    app.state.content_types = chunks_table.column("content_type").to_pylist()
    app.state.token_counts = chunks_table.column("token_count").to_pylist()
    app.state.num_chunks_arr = chunks_table.column("num_chunks").to_pylist()
    app.state.chunk_indices_arr = chunks_table.column("chunk_index").to_pylist()
    app.state.id_to_idx = {cid: i for i, cid in enumerate(app.state.chunk_ids)}

    # Startup caches from DuckDB
    t_cache = time.time()
    db = app.state.duckdb

    # iu_series_map: canonical_id → frozenset of series (for boost/filter)
    cur = db.cursor()
    rows = cur.execute(
        "SELECT canonical_id, appearances FROM canonical_ius"
    ).fetchall()
    cur.close()
    iu_series_map = {}
    for canonical_id, appearances in rows:
        apps = json.loads(appearances) if appearances and appearances != "[]" else []
        series_set = frozenset(a.get("series", "") for a in apps if isinstance(a, dict))
        iu_series_map[canonical_id] = series_set
    app.state.iu_series_map = iu_series_map
    logger.info("  iu_series_map: %d entries", len(iu_series_map))

    # iu_to_chunk_indices: base miuid → numpy int32 array of chunk indices
    iu_to_chunk_lists: dict[str, list[int]] = {}
    for i, cid in enumerate(app.state.chunk_ids):
        iu_id = cid.rsplit("_c", 1)[0]
        base_miuid = iu_id
        if base_miuid.endswith(("_v1", "_v2", "_v3", "_v4", "_v5")):
            base_miuid = base_miuid.rsplit("_v", 1)[0]
        iu_to_chunk_lists.setdefault(base_miuid, []).append(i)
    iu_to_chunk_indices = {k: np.array(v, dtype=np.int32) for k, v in iu_to_chunk_lists.items()}
    del iu_to_chunk_lists
    app.state.iu_to_chunk_indices = iu_to_chunk_indices
    logger.info("  iu_to_chunk_indices: %d IUs", len(iu_to_chunk_indices))

    logger.info("  Caches built in %.1fs", time.time() - t_cache)
    logger.info("Resources loaded.")
    yield

    app.state.duckdb.close()
    if app.state.pg_pool:
        await app.state.pg_pool.close()
    app.state.qdrant.close()


# --- App ---

app = FastAPI(title="CNH Technician Assistant", lifespan=lifespan)


# --- Auth ---

bearer_scheme = HTTPBearer()


async def verify_bearer_token(
    request: Request,
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
):
    pool = request.app.state.pg_pool
    if pool is None:
        raise HTTPException(500, "DATABASE_URL not configured")
    token_hash = hashlib.sha256(credentials.credentials.encode()).hexdigest()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            'SELECT 1 FROM "Token" WHERE "tokenHash" = $1 AND "isRevoked" = false',
            token_hash,
        )
    if row is None:
        raise HTTPException(401, "Invalid or revoked token")


# --- Routes ---

@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/query", response_model=QueryResponse, dependencies=[Depends(verify_bearer_token)])
async def query(req: QueryRequest):
    t0 = time.time()
    spans = []
    model_calls = []

    is_followup = req.conversation is not None and req.sources is not None
    conversation_id = req.conversation_id or str(uuid.uuid4())
    logger.info("conversation_id=%s followup=%s", conversation_id, is_followup)

    if is_followup:
        # --- Follow-up turn: skip retrieval, reconstruct context ---
        expanded_queries = req.expanded_queries or []

        # Reconstruct ranked_chunks from cached sources
        ranked = []
        for i, src in enumerate(req.sources):
            iu_id = src.get("iu_id", "")
            chunk_id = iu_id + "_c000"
            score = 1.0 / (i + 1)  # synthetic RRF-like score by position
            ranked.append((chunk_id, score))

        t = time.time()
        context, sources = await asyncio.to_thread(
            assemble_context,
            ranked,
            app.state.chunk_ids,
            app.state.texts,
            app.state.content_types,
            app.state.token_counts,
            app.state.id_to_idx,
            app.state.num_chunks_arr,
            app.state.chunk_indices_arr,
            series_filter=req.series,
            iu_series_map=app.state.iu_series_map,
            db=app.state.duckdb,
        )
        spans.append(Span(name="context_assembly", duration_ms=int((time.time() - t) * 1000)))

        retrieval_ms = 0
        vin_info = None

        # Generation with conversation history
        t_gen = time.time()
        conversation_dicts = [turn.model_dump() for turn in req.conversation]
        answer, questions, gen_mc = await generate_answer(
            app.state.anthropic, req.query, context, sources,
            model=req.model, conversation=conversation_dicts,
        )
        generation_ms = int((time.time() - t_gen) * 1000)
        spans.append(Span(name="generation", duration_ms=generation_ms))
        model_calls.append(ModelCall(**gen_mc))

    else:
        # --- Turn 1: full retrieval pipeline ---
        vin_info = None
        tt_id = None
        machine_id = req.identifier or req.vin

        t_pre = time.time()
        if machine_id:
            vin_coro = asyncio.to_thread(resolve_identifier, machine_id, db=app.state.duckdb)
            expansion_coro = expand_query(app.state.openai, req.query)
            results = await asyncio.gather(vin_coro, expansion_coro, return_exceptions=True)

            # VIN result
            vin_result = results[0]
            if isinstance(vin_result, Exception):
                logger.warning("Identifier resolution failed: %s", vin_result)
            else:
                vin_info = vin_result
                tt_id = vin_info["tt_id"]
            spans.append(Span(name="vin_resolution", duration_ms=int((time.time() - t_pre) * 1000)))

            # Expansion result
            exp_result = results[1]
            if isinstance(exp_result, Exception):
                logger.warning("Query expansion failed: %s", exp_result)
                expansions, expansion_mc = [], None
            else:
                expansions, expansion_mc = exp_result
            spans.append(Span(name="query_expansion", duration_ms=int((time.time() - t_pre) * 1000)))
        else:
            expansions, expansion_mc = await expand_query(app.state.openai, req.query)
            spans.append(Span(name="query_expansion", duration_ms=int((time.time() - t_pre) * 1000)))

        if expansion_mc:
            model_calls.append(ModelCall(**expansion_mc))
        all_queries = [req.query] + expansions
        expanded_queries = expansions

        # Hybrid retrieval
        t_retrieval = time.time()
        ranked, retrieval_spans, retrieval_mcs, applicable_miuids = await hybrid_search(
            all_queries,
            app.state.openai,
            app.state.qdrant,
            app.state.bm25,
            app.state.chunk_ids,
            series_filter=req.series,
            series_only=req.series_only,
            tt_id=tt_id,
            tt_only=req.vin_only,
            db=app.state.duckdb,
            iu_series_map=app.state.iu_series_map,
            iu_to_chunk_indices=app.state.iu_to_chunk_indices,
        )
        retrieval_ms = int((time.time() - t_retrieval) * 1000)
        spans.extend(Span(**s) for s in retrieval_spans)
        model_calls.extend(ModelCall(**mc) for mc in retrieval_mcs)

        # Context assembly
        t = time.time()
        context, sources = await asyncio.to_thread(
            assemble_context,
            ranked,
            app.state.chunk_ids,
            app.state.texts,
            app.state.content_types,
            app.state.token_counts,
            app.state.id_to_idx,
            app.state.num_chunks_arr,
            app.state.chunk_indices_arr,
            series_filter=req.series,
            iu_series_map=app.state.iu_series_map,
            db=app.state.duckdb,
            applicable_miuids=applicable_miuids,
        )
        spans.append(Span(name="context_assembly", duration_ms=int((time.time() - t) * 1000)))

        # Generation
        t_gen = time.time()
        answer, questions, gen_mc = await generate_answer(
            app.state.anthropic, req.query, context, sources, model=req.model,
        )
        generation_ms = int((time.time() - t_gen) * 1000)
        spans.append(Span(name="generation", duration_ms=generation_ms))
        model_calls.append(ModelCall(**gen_mc))

    total_ms = int((time.time() - t0) * 1000)

    # Parse questions into Pydantic models if present
    parsed_questions = None
    if questions:
        parsed_questions = [Question(**q) for q in questions]

    return QueryResponse(
        answer=answer,
        questions=parsed_questions,
        sources=sources,
        vin_info=vin_info,
        timing={
            "retrieval_ms": retrieval_ms,
            "generation_ms": generation_ms,
            "total_ms": total_ms,
        },
        spans=spans,
        model_calls=model_calls,
        conversation_id=conversation_id,
        expanded_queries=expanded_queries,
    )
