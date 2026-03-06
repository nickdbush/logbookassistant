# CNH Technician Assistant

RAG pipeline that transforms CNH dealer technical documentation (Arbortext XML)
into a searchable corpus for technician Q&A.

**Source data**: 1,476 series of encrypted Arbortext XML files (~2.5M IU files)
→ deduplicated to ~930K unique IUs → chunked to ~1.58M retrieval chunks.

## Quick start

```bash
# Query the assistant (requires Qdrant running on localhost:6333)
.venv/bin/python scripts/assistant.py "How to diagnose fault code 523774"
.venv/bin/python scripts/assistant.py "hydraulic pump pressure test" --series A.A.01.034
.venv/bin/python scripts/assistant.py "SCR aftertreatment DPF regen" --verbose

# Filter by VIN (resolves to technical type, boosts applicable IUs 3x)
.venv/bin/python scripts/assistant.py "hydraulic fault" --vin HACT7210VPD100757
.venv/bin/python scripts/assistant.py "hydraulic fault" --vin HACT7210VPD100757 --vin-only
```

## Pipeline

Scripts are run in order. Each reads the output of the previous stage.
Source data lives on an external drive at `/Volumes/logbookdata/`.

### Stage 1: Extract & deduplicate

```bash
# Scan all series, hash all EN IU files, build source mapping + dedup report
.venv/bin/python scripts/profile_dedup.py -n 1476

# Resolve multi-hash IU IDs into a canonical set
.venv/bin/python scripts/build_canonical.py
```

**Outputs**: `data/iu_source_mapping.json`, `data/canonical_iu_mapping.parquet`

### Stage 2: Convert

```bash
# Decrypt + parse XML → extract metadata → convert to HTML → Markdown
.venv/bin/python scripts/convert_corpus.py
```

**Output**: `data/corpus/canonical_ius.parquet` (930K rows, ~1.8 GB)

### Stage 3: Enrich

```bash
# Flatten metadata JSON, add derived columns (has_tables, estimated_tokens, etc.)
.venv/bin/python scripts/enrich_corpus.py
```

**Output**: `data/corpus/canonical_ius_enriched.parquet`

### Stage 4: Document structure

```bash
# Extract document → IU ordering from doc/ wrapper files
.venv/bin/python scripts/build_doc_structure.py
```

**Output**: `data/document_structure.parquet`

### Stage 5: Chunk

```bash
# Split large IUs on heading boundaries, keep small IUs whole
.venv/bin/python scripts/chunk_corpus.py
```

**Output**: `data/corpus/chunks.parquet` (1.58M chunks)

### Stage 6: Embed

```bash
# Embed chunks via OpenAI text-embedding-3-small (requires OPENAI_API_KEY in .env)
.venv/bin/python scripts/embed_batch.py --dry-run   # check cost first
.venv/bin/python scripts/embed_batch.py
```

**Outputs**: `data/corpus/embeddings.npy` (~9.2 GB), `data/corpus/chunk_ids.npy`

### Stage 7: TT applicability index

```bash
# Build IU-to-technical-type applicability from scripts.zip (requires source volume)
.venv/bin/python scripts/build_tt_index.py
```

**Outputs**: `data/iu_tt_applicability.parquet` (48.6M pairs), `data/technical_types.parquet` (23.8K TTs)

### Stage 8: Index

```bash
# Start Qdrant (Docker required)
docker run -d -p 6333:6333 -p 6334:6334 \
  -v $(pwd)/data/qdrant_storage:/qdrant/storage qdrant/qdrant

# Build vector store + BM25 index + DuckDB metadata
.venv/bin/python scripts/build_vector_store.py
.venv/bin/python scripts/build_metadata_db.py
```

**Outputs**: Qdrant collection `chunks`, `data/bm25_index.pkl`, `data/metadata.duckdb`

### Stage 9: Query

```bash
.venv/bin/python scripts/assistant.py "your question here"
.venv/bin/python scripts/assistant.py "hydraulic fault" --vin HACT7210VPD100757
```

## Project structure

```
scripts/
  lib/
    decrypt.py          # Blowfish decrypt + zip helpers
    metadata.py         # XML metadata extraction (fault codes, parts, etc.)
    xml_to_html.py      # Arbortext XML → HTML conversion engine
    perfsql.py          # UTF-16BE perfsql file parser
    vin.py              # VIN → technical type resolution (S3 + CNH API)
  profile_dedup.py      # Stage 1: cross-series dedup profiling
  build_canonical.py    # Stage 1: canonical IU resolution
  convert_corpus.py     # Stage 2: full corpus conversion
  enrich_corpus.py      # Stage 3: metadata flattening + profiling
  build_doc_structure.py# Stage 4: document→IU ordering
  chunk_corpus.py       # Stage 5: chunking for RAG
  embed_batch.py        # Stage 6: OpenAI embeddings
  build_tt_index.py     # Stage 7: TT applicability index from scripts.zip
  build_vector_store.py # Stage 8: Qdrant + BM25
  build_metadata_db.py  # Stage 8: DuckDB metadata
  assistant.py          # Stage 9: RAG assistant (entrypoint)
  test_retrieval.py     # Sanity check: run test queries against indices
config/
  tag_map.yaml          # Arbortext XML tag → HTML mapping (236 tags)
docs/
  ice_codes.md          # ICE code taxonomy reference
  scripts_data.md       # scripts.zip data model reference
  srt.md                # Standard Repair Times reference
  vin_resolution.md     # VIN → Technical Type resolution reference
data/                   # Generated data (gitignored, ~25 GB total)
```

## Key design decisions

- **English only**: process `iu/EN/` files, ignore other languages
- **Extract metadata before HTML conversion**: fault codes, part numbers, and
  cross-references lose their semantic tag info after XML→HTML
- **Exact dedup by SHA-256**: 60.7% reduction across series
- **Canonical resolution**: normalize (strip Arbortext comments/PIs) before
  hashing to collapse trivial dups; Jaccard similarity to distinguish revisions
  from genuinely different documents sharing the same IU ID
- **Hybrid retrieval**: vector (cosine) + BM25 (keyword), fused with RRF
- **Query expansion**: gpt-4o-mini generates 3 alternative queries for broader recall
- **VIN-based filtering**: resolve VIN → technical type via S3 shard or CNH Store
  API, then boost/filter IUs by TT applicability from `WebDocIu` tables

## Deployment

The RAG assistant runs on a dedicated Hetzner Cloud server (CAX41 — 16 vCPU ARM,
32 GB RAM, 320 GB NVMe, Ubuntu 24.04). The main Logbook app's Lambda API
(`api.joinlogbook.com`) proxies assistant requests to this server.

### Why not AWS?

The assistant is a stateful, memory-heavy process (~15-20 GB resident) with ~90s
cold start. Lambda can't run it (10 GB max, 15 min timeout, cold starts on every
invocation). An equivalent EC2/ECS instance costs 4-5x more than Hetzner for this
workload, and the service has no need for VPC access or AWS-specific integrations —
it reads local indices and calls the OpenAI API.

### Server setup

The server (`logbookdata` in SSH config → `assistant@46.225.89.63`) was configured:

- Root login and password auth disabled in sshd
- Docker installed, `assistant` added to `docker` group
- App directory at `/opt/logbook-rag/`
- UFW firewall: SSH (22), HTTP (80), HTTPS (443) only

### Required data files

Only a subset of the pipeline output is needed at runtime (~14 GB):

| File | Size | Purpose |
|------|------|---------|
| `data/bm25_index.pkl` | 1.8 GB | BM25 keyword retrieval |
| `data/metadata.duckdb` | 11 GB | IU metadata, series boost, TT applicability |
| `data/corpus/chunks.parquet` | 683 MB | Chunk text + token counts |
| `data/qdrant_storage/` | ~500 MB | Qdrant vector index |

Sync command:

```bash
rsync -avz --progress \
  --include='bm25_index.pkl' \
  --include='metadata.duckdb' \
  --include='corpus/' \
  --include='corpus/chunks.parquet' \
  --include='qdrant_storage/***' \
  --exclude='corpus/*' \
  --exclude='*' \
  data/ logbookdata:/opt/logbook-rag/data/
```

### Services (Docker Compose)

Three containers in `/opt/logbook-rag/docker-compose.yml`:

- **qdrant** — vector search, internal only (no exposed ports)
- **api** — FastAPI wrapper around `assistant.py` logic, port 8080 internal
- **caddy** — reverse proxy with automatic Let's Encrypt TLS on ports 80/443

The API authenticates requests via a shared secret (`X-Api-Key` header) known
to both the Lambda backend and this server.

### Updating the corpus

When the pipeline is re-run (new series data, re-embedding, etc.):

1. Rebuild locally: run pipeline stages 1-8
2. Stop services on server: `ssh logbookdata 'cd /opt/logbook-rag && docker compose down'`
3. Rsync the 4 required files (see above)
4. Restart: `ssh logbookdata 'cd /opt/logbook-rag && docker compose up -d'`

### Query flow

1. Lambda receives a technician query from the app
2. Lambda calls `POST https://assistant.joinlogbook.com/query` with API key
3. Server resolves VIN → technical type (S3 shard → CNH Store API fallback)
4. Query expansion via gpt-4o-mini (3 alternatives)
5. Hybrid retrieval: Qdrant vector search + BM25 keyword, RRF fusion
6. Series/TT boost (3x score for applicable IUs)
7. Context assembly (top-15 chunks within 10K token budget, adjacent chunk expansion)
8. Generation via gpt-4o with technical system prompt
9. Response returned to Lambda → app
