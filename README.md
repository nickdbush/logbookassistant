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
