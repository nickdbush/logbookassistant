"""FastAPI app for CNH technician RAG assistant."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import pickle
import time
from contextlib import asynccontextmanager
from pathlib import Path

import asyncpg
import pyarrow.parquet as pq
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
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

class QueryRequest(BaseModel):
    query: str
    series: str | None = None
    series_only: bool = False
    identifier: str | None = None
    vin: str | None = None  # backward compat alias for identifier
    vin_only: bool = False
    model: str = "gpt-5-mini"


class QueryResponse(BaseModel):
    answer: str
    sources: list[dict]
    vin_info: dict | None = None
    timing: dict


# --- Lifespan ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Loading resources...")

    app.state.openai = AsyncOpenAI()
    app.state.qdrant = QdrantClient(url=QDRANT_URL)

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

    logger.info("Resources loaded.")
    yield

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

    # Identifier resolution (VIN or VRM)
    vin_info = None
    tt_id = None
    machine_id = req.identifier or req.vin
    if machine_id:
        try:
            vin_info = await asyncio.to_thread(resolve_identifier, machine_id)
            tt_id = vin_info["tt_id"]
        except (ValueError, Exception) as e:
            logger.warning("Identifier resolution failed: %s", e)

    # Query expansion
    expansions = await expand_query(app.state.openai, req.query)
    all_queries = [req.query] + expansions

    # Hybrid retrieval
    t_retrieval = time.time()
    ranked = await hybrid_search(
        all_queries,
        app.state.openai,
        app.state.qdrant,
        app.state.bm25,
        app.state.chunk_ids,
        series_filter=req.series,
        series_only=req.series_only,
        tt_id=tt_id,
        tt_only=req.vin_only,
    )
    retrieval_ms = int((time.time() - t_retrieval) * 1000)

    # Context assembly
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
    )

    # Generation
    t_gen = time.time()
    answer = await generate_answer(app.state.openai, req.query, context, sources, model=req.model)
    generation_ms = int((time.time() - t_gen) * 1000)

    total_ms = int((time.time() - t0) * 1000)

    return QueryResponse(
        answer=answer,
        sources=sources,
        vin_info=vin_info,
        timing={
            "retrieval_ms": retrieval_ms,
            "generation_ms": generation_ms,
            "total_ms": total_ms,
        },
    )
