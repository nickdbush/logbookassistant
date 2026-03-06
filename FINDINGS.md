# Findings

## 2026-03-04 — decrypt_test.py, profile_series.py (series A.A.01.034)

### Decryption
- Files are Blowfish ECB encrypted (24 null-byte key), PKCS5 padded, then zlib compressed.
- Pipeline: read from zip → decrypt → unpad → zlib decompress → UTF-8 XML.
- 3 Russian IU files failed XML parse (malformed after decryption) out of 197,612 total.

### File structure inside docs.zip
- `doc/` — 1,081 document wrappers. Root tag `<Document>` contains `<Chapter>` / `<IU_Set>` / `<Master_IU_Ref>` pointing to IUs.
- `iu/EN/`, `iu/RU/`, etc. — 196,528 IU files across multiple languages.
- Also contains `.rdf`, `.list`, `.txt` files (not profiled yet).

### IU types (7 root tags for IU files)
- Diagnostic_IU: 61,987 (largest category)
- Functional_Data_IU: 42,776
- Service_IU: 35,058
- Operating_IU: 26,725
- General_IU: 15,376
- Technical_Data_IU: 9,688
- ServiceBulletin_IU: 4,918

### Tag census highlights (237 unique tags)
- Dominated by tabular content: `Text` (15M), `entry` (11M), `row` (2.8M)
- Heavy configuration/variant markup: `Configuration` (2.2M), `Part_Set` (1.5M)
- Rich image annotation: `Image_Config`, `Image_Compound`, `Image_Annotation` (~791K each)
- Only 3 mixed-content tags: `FMRef`, `Text`, `Text_Config`
- Max nesting depth: 27 levels
- Arbortext processing instructions present (`<?Pub _font ...?>`)

### Sample conversion (51 IUs, all 7 types)
- Full pipeline XML → metadata → HTML → Markdown runs cleanly on all 51 samples — 0 failures, 0 unknown tags.
- All 236 tags mapped in `config/tag_map.yaml`. No unmapped tags encountered in sample.
- CALS tables convert well: colspans from `namest`/`nameend`, `thead` → `<th>`, alignment preserved.
- Physical data renders metric-only inline: "120.0 L/min", "215.0 bar", ranges as "100.0–120.0 L/min".
- Configuration `data-configdata` attributes preserved in HTML, text content flows into markdown.
- Images render as `<img>` with fileref/IFS IDs — actual image files not in the zip (referenced externally).
- First 8 Diagnostic_IUs sampled were troubleshooting-style (no FCR chains). FCR/fault code extraction code exists but needs validation against IUs that contain `Fault_Code_Resolution` blocks.
- `markdownify` handles tables, headings, lists, blockquotes well. Some empty columns appear in wide tables due to colspan layout.
- Processing instructions (`<?Pub ...?>`) stripped cleanly via regex.

### Cross-series dedup profiling (all 1,476 series)
- **60.7% exact dedup** across all series: 2,455,037 total IU files → 963,875 unique hashes, 925,180 unique IU IDs.
- 300 of 1,476 series had 0 EN IU files (1,176 active).
- **ServiceBulletin_IU: 98% dedup** (385,090 → 5,836 unique) — massively broadcast.
- All other types show 48-57% dedup at full scale (much higher than the 5-15% seen in 100-series sample):
  - Diagnostic_IU: 57%, Service_IU: 55%, General_IU: 54%, Operating_IU: 50%, Functional_Data_IU: 48%, Technical_Data_IU: 56%.
- **Filename-based dedup is mostly reliable** but 3.9% of IU IDs (35,873/925,180) have multiple hashes across series. Investigated these exceptions in depth:
  - ~48% are **trivial**: collapse to identical content after stripping Arbortext comment (`<!--Arbortext, Inc., 1988-20XX-->`) and processing instructions (`<?Pub ...?>`). ~17K IU IDs.
  - ~26% are **minor** (1-5% character diff): small attribute tweaks, `confmig` values, internal IDs.
  - ~11% are **moderate** (5-20% diff): updated sections, added/removed content blocks.
  - ~15% are **major** (>20% diff): genuinely different documents sharing the same IU ID — different safety messages, completely rewritten procedures. These are real revisions, not duplicates.
  - Median similarity after normalization: 98.9%. Mean: 91.5%.
  - **Dedup strategy**: normalize (strip Arbortext comments + PIs) before hashing to collapse trivial dups (~17K). For real multi-version IU IDs (~18K), keep the longest/most complete version. Near-dedup (MinHash) deferred to post-conversion for template family detection.
- Distribution of unique IUs across series: 50% appear in 1 series, 37% in 2-3, 6% in 4-5, 5% in 6-10, 1.4% in 11-50, 0.1% in 51+.
- 3 unexpected root tags: `UNKNOWN` (2 unique), `Fault_Code_Resolution` (1 unique, standalone).

### Canonical IU resolution & full conversion
- **Canonical resolution**: 925,180 unique IU IDs → 930,082 canonical entries (net ~5K added from major-variant splits).
  - Multi-hash resolution used Jaccard similarity on whitespace tokens (≥0.80 threshold) to distinguish revisions from genuinely different documents.
- **Full conversion**: 930,080 successful (2 XML parse failures, 0.0002% error rate), completed in 6.1 minutes with 10 workers.
- **Output**: `data/corpus/canonical_ius.parquet` — 1.83 GB, 930,082 rows.
- **Markdown length**: min 34, median 1,633, mean 2,985, P95 9,258, max 605,683 chars.
- Conversion was much faster than estimated (~6 min vs ~13 hrs) — I/O from external drive was not the bottleneck; CPU-bound decrypt+parse+convert was well parallelized.

### Enrichment & corpus profiling
- **Enriched parquet**: `data/corpus/canonical_ius_enriched.parquet` — 1.81 GB, 930,082 rows. Metadata JSON flattened into queryable columns, original `metadata` column dropped.
- **Metadata coverage** (% of 930K IUs with non-empty values):
  - `fault_codes`: 25.4% (235,864), `fcr_chains`: 25.3% (235,408) — nearly 1:1 overlap, as expected.
  - `iu_cross_references`: 38.1% (354,189) — 1.36M total cross-refs, avg 3.8 refs/IU.
  - `configuration`: 7.1% (66,097), `consumable_references`: 4.5% (42,050), `warranty_codes`: 0.1% (515).
  - `part_numbers`: 5.9% (55,332), `tool_references`: 1.6% (14,506). Initially extracted as 0% due to wrong assumptions in `metadata.py`: `Part_Reference` stores the part number as text content (not in a `Part_Number` child), and `Tool_Reference` uses `STName` attribute for name and text content for part number (not a `part_number` attribute). Fixed.
- **Content features**: 41.9% have tables, 37.0% have images.
- **Estimated tokens**: 694M total, 746 avg/IU (using len(md)/4 heuristic).
- **Large IUs** (>50K chars): 1,758 IUs, dominated by wire connector component diagrams (Functional_Data_IU). Largest is 605K chars. These will need splitting at chunk stage.

### Document structure
- **Output**: `data/document_structure.parquet` — 37,665 unique documents from 459,704 raw doc files across all series.
- **Document types**: SB (service bulletins, 17,070), OM (operator manuals, 10,295), II (installation instructions, 9,047), SG (451), AI (420), TR (technical references, 382).
- **IUs per document**: median 5, mean 61 (skewed by large TRs up to 3,765 IUs). TR docs are the hierarchical chapter-based manuals; SB docs typically reference 1-2 IUs.
- **Reuse**: mean 12 series appearances per document, max 596. Median 1 — many docs appear in only one series.
- **48 empty docs** with zero IU references (empty wrappers, likely placeholders).
- **miuid mapping**: miuids from `Master_IU_Ref` map directly to IU filenames (no suffix stripping needed). For IUs that got `_v1`/`_v2` suffixes during canonicalization, the base ID matches.

### Chunking
- **Output**: `data/corpus/chunks.parquet` — 1,578,246 chunks (1.70x expansion), 0.72 GB.
- **17 IUs skipped** (< 10 tokens — empty or heading-only stubs).
- **93.7% of IUs kept whole** (≤2000 estimated tokens). Only 58,831 IUs (6.3%) needed splitting.
- **Splitting strategy**: heading-based (`##`/`###`/`####`) with ancestor heading context prepended. Fallback: table-row splitting for large tables, paragraph splitting for prose.
- **Chunk token distribution**: min 10, median 271, mean 455, P95 1,495, max 3,736.
- **Total tokens**: 718M (3.5% overhead from context prefixes vs 694M pre-chunking).
- **54 chunks exceed 2500 tokens** — all wide tables or long prose under a single heading. Acceptable edge cases.
- **Wire connector IUs**: split cleanly on `####` per-connector headings (430-457 chunks each). Top 10 IUs by chunk count are all wire connector diagrams.

### Embedding & retrieval
- **Embedding model**: `text-embedding-3-small` (1536 dims), real-time API with async concurrency and TPM rate limiting.
- **Corpus**: 1,578,246 chunks, ~718M tokens. Estimated cost ~$14.36 at real-time pricing ($0.020/1M tokens). Batch API would halve this but adds latency.
- **Output**: `embeddings.npy` — float32 memmap, shape (1,578,246 × 1536), ~9.2 GB.
- **Vector store**: Qdrant running in Docker (ports 6333/6334). Local-mode Qdrant emits warnings and degrades above 20K points — Docker required at this scale. gRPC transport (`prefer_grpc=True`) and 10K-point upload batches are significantly faster than REST with 1K batches.
- **BM25 index**: `rank_bm25.BM25Okapi`, whitespace-tokenized lowercase text, serialized to `data/bm25_index.pkl`.
- **Metadata store**: DuckDB at `data/metadata.duckdb` with `canonical_ius`, `document_structure`, `chunks` tables loaded from parquet.
- **Retrieval sanity check** (5 queries, top-5 from vector + BM25):
  - Vector search returns semantically relevant results (cosine scores 0.62–0.77). Exact-match queries like specific fault codes may not find the literal code but surface structurally similar diagnostics.
  - BM25 returns keyword-matched results (scores 22–37). Strong for exact terms but can drift semantically (e.g., "DPF regeneration" query pulling general engine advice).
  - Content types in results align with expectations: diagnostic queries → Diagnostic_IU, service procedures → Service_IU, specs → Technical_Data_IU.
  - Hybrid retrieval (combining both) will be needed for production to get both semantic and keyword coverage.

### RAG assistant (`scripts/assistant.py`)
- **Pipeline**: query expansion (gpt-4o-mini, 3 alternatives) → hybrid retrieval (vector + BM25, RRF fusion) → context assembly with adjacent chunk expansion → generation (gpt-4o or gpt-5.2).
- **Retrieval window matters**: default top-20 per query×method is sufficient without series filtering, but when `--series` is used, the candidate pool must be widened to top-200 — otherwise the series filter/boost finds zero matches in 1.58M chunks.
- **Series boost**: 3x RRF score multiplier for chunks whose parent IU appears in the target series. `--series-only` hard-filters instead of boosting.
- **Document structure lookup unreliable**: `document_structure.iu_ids` doesn't reliably contain all IU IDs (many IUs present in `canonical_ius.appearances` are absent from `document_structure`). Series info from `canonical_ius.appearances` is the reliable source.
- **Generation quality**: gpt-4o produces good structured diagnostic answers from retrieved context. Citation precision varies — sometimes cites every source on every step rather than mapping specific sources to specific claims. This is a prompt tuning issue, not a retrieval problem.
- **Load time**: ~90s startup dominated by BM25 pickle (1.9 GB) and parquet read (~1 GB). Acceptable for CLI; a persistent server would be needed for interactive use.
- **`max_tokens` vs `max_completion_tokens`**: newer OpenAI models (gpt-5.2+) require `max_completion_tokens` parameter; `max_tokens` returns a 400 error.

### TT applicability index (`scripts/build_tt_index.py`)
- **Input**: `scripts.zip` from each series folder (alongside `docs.zip`). Contains UTF-16BE `.perfsql` files — SQL INSERT statements defining relational data (product hierarchy, technical types, IU-to-document mappings).
- **Processing**: 706 of 1,476 series processed (770 skipped — missing ViewProducts, TechnicalType, or WebDocIu tables). 10 workers, 168s total.
- **Output**: 48,655,603 unique `(iu_miuid, tt_id)` applicability pairs across 23,835 unique technical types.
  - `data/iu_tt_applicability.parquet` — 476.5 MB
  - `data/technical_types.parquet` — 0.4 MB
- **Applicability logic**: `WebDocIu` rows have `iu_app_mod` and `iu_app_tt` columns (ICE codes). NULL means "applies to all models/TTs". Non-null values are matched against `model_icecode` and `tt_icecode` sets from `ViewProducts`, grouped by `tt_id`. Only active TTs (`TechnicalType.a_status == 1`) are included.
- **DuckDB size impact**: loading 48.6M applicability rows into DuckDB inflated the database from ~20 MB to 11.5 GB. The `iu_miuid` string column doesn't compress well in DuckDB vs parquet (476 MB). An indexed `tt_id` lookup takes <100ms at runtime, which is acceptable. Could switch to direct parquet queries if the DB size becomes a problem.
- **perfsql format**: UTF-16BE with BOM, INSERT line defines columns, data rows are `(val1, 'val2', null)` tuples. Strings use `\'`, `\\`, `\n`, `\r`, `\t` escapes. Numbers are unquoted. `A_STATUS` comes back as int (not string). Empty strings (`''`) are distinct from `null`.

### IU titles
- **99.9998% of IUs have a usable title** extracted from the first markdown heading in `content_md` (only 2 of 930,082 IUs lack one).
- Titles are descriptive and human-readable: e.g., "Engine - Overview", "Turbocharger oil supply line - Remove", "Engine - General specification".
- Currently extracted post-conversion from the markdown heading. In future, titles should be extracted directly from the original Arbortext XML (`Full_Title`, `Title`, `Header` elements) before HTML conversion — this would preserve the semantic distinction between primary and secondary titles, and avoid dependence on the conversion pipeline.
- Title added as a dedicated column in enriched parquet and surfaced in the API sources list.

### Open questions
- What do the non-EN language IU files contain? Translations or distinct content?
- What's in the .rdf and .list files?
