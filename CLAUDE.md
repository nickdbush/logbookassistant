# CNH Technician Assistant Pipeline

Transforms CNH dealer technical documentation (Arbortext XML)
into a clean, deduplicated, enriched corpus for RAG retrieval.

## Working Notes
Maintain FINDINGS.md with discoveries about the data as you go.
Only update the findings at the end of a session.
Do not duplicate findings in this document, and edit old findings as you learn more.

## Environment
- Python virtual environment: `.venv/` — always use `.venv/bin/python` to run scripts

## Data Location & Format
- Source: /Volumes/logbookdata/cnh/iso/repository/AGCE/data/series
- 1479 series folders, each contains docs.zip
- ZIPs contain Blowfish-encrypted XML files
- Decryption: Blowfish ECB, key = 24 null bytes (0x00 * 24), PKCS5 padding
- After decrypt+unpad, data is zlib-compressed — must inflate before parsing
- Decrypted/inflated files are Arbortext XML (proprietary tags, no schema)

## Document Structure
- A Document is a thin wrapper over an ordered list of IUs
  (Information Units)
- ~20k IUs per series, heavy duplication across series
- IUs contain: diagnostic procedures, repair steps, spec tables,
  fault codes, wiring descriptions

## Pipeline Stages
1. Extract: decrypt and parse XML from all series
2. Profile: census of XML tags, IU structure, duplication rates
3. Transform: Arbortext XML → HTML → Markdown
4. Deduplicate: exact (SHA-256) then near-dupe (MinHash)
5. Enrich: extract fault codes, part numbers, cross-refs,
   classify content type
6. Chunk: respect IU boundaries, split large IUs on structure
7. Output: canonical_ius.parquet + chunks.parquet

## Key Decisions
- English only: IU files exist in multiple languages (EN, RU, AR, etc.).
  Process only English — prefer en-GB, fall back to any EN variant.
  IU path pattern: `iu/EN/`
- Convert XML → HTML (tag renaming + structural fixups)
  then use markdownify for HTML → Markdown
- Extract metadata BEFORE converting to HTML (fault codes,
  part numbers, cross-refs lose semantic tag info)
- Preserve mapping: each unique IU → list of
  (series, document_id, position) appearances

## Reference Documentation
- `docs/ice_codes.md` — ICE code taxonomy (product hierarchy + documentation classification)
- `docs/scripts_data.md` — scripts.zip data model (SQL tables linking IUs, documents, models, VINs)
- `docs/srt.md` — Standard Repair Times (operation codes, time units, inclusion hierarchy)
- `docs/vin_resolution.md` — VIN → Technical Type resolution (build process, shard lookup, runtime fallback)
