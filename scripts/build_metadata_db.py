"""Build DuckDB metadata store from parquet files.

Creates data/metadata.duckdb with tables:
- canonical_ius (from canonical_ius_enriched.parquet)
- document_structure (from document_structure.parquet)
- chunks (from chunks.parquet, excluding text column)
"""

from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent

DB_PATH = ROOT / "data" / "metadata.duckdb"
CANONICAL_IUS_PATH = ROOT / "data" / "corpus" / "canonical_ius_enriched.parquet"
DOC_STRUCTURE_PATH = ROOT / "data" / "document_structure.parquet"
CHUNKS_PATH = ROOT / "data" / "corpus" / "chunks.parquet"
TT_PATH = ROOT / "data" / "technical_types.parquet"
TT_APP_PATH = ROOT / "data" / "iu_tt_applicability.parquet"


def main():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Remove existing DB to start fresh
    if DB_PATH.exists():
        DB_PATH.unlink()

    con = duckdb.connect(str(DB_PATH))

    # canonical_ius
    print(f"Loading canonical_ius from {CANONICAL_IUS_PATH.name}...")
    con.execute(f"""
        CREATE TABLE canonical_ius AS
        SELECT * FROM read_parquet('{CANONICAL_IUS_PATH}')
    """)
    count = con.execute("SELECT count(*) FROM canonical_ius").fetchone()[0]
    print(f"  {count:,} rows")

    # document_structure
    print(f"Loading document_structure from {DOC_STRUCTURE_PATH.name}...")
    con.execute(f"""
        CREATE TABLE document_structure AS
        SELECT * FROM read_parquet('{DOC_STRUCTURE_PATH}')
    """)
    count = con.execute("SELECT count(*) FROM document_structure").fetchone()[0]
    print(f"  {count:,} rows")

    # chunks (skip text column)
    print(f"Loading chunks from {CHUNKS_PATH.name} (excluding text)...")
    con.execute(f"""
        CREATE TABLE chunks AS
        SELECT chunk_id, canonical_iu_id, chunk_index, num_chunks,
               token_count, content_type, fault_codes, part_numbers,
               tool_references
        FROM read_parquet('{CHUNKS_PATH}')
    """)
    count = con.execute("SELECT count(*) FROM chunks").fetchone()[0]
    print(f"  {count:,} rows")

    # technical_types (optional — only if parquet exists)
    if TT_PATH.exists():
        print(f"Loading technical_types from {TT_PATH.name}...")
        con.execute(f"""
            CREATE TABLE technical_types AS
            SELECT * FROM read_parquet('{TT_PATH}')
        """)
        count = con.execute("SELECT count(*) FROM technical_types").fetchone()[0]
        print(f"  {count:,} rows")
    else:
        print(f"Skipping technical_types ({TT_PATH.name} not found)")

    # iu_tt_applicability (optional)
    if TT_APP_PATH.exists():
        print(f"Loading iu_tt_applicability from {TT_APP_PATH.name}...")
        con.execute(f"""
            CREATE TABLE iu_tt_applicability AS
            SELECT * FROM read_parquet('{TT_APP_PATH}')
        """)
        con.execute("CREATE INDEX idx_tt_app ON iu_tt_applicability(tt_id)")
        count = con.execute("SELECT count(*) FROM iu_tt_applicability").fetchone()[0]
        print(f"  {count:,} rows")
    else:
        print(f"Skipping iu_tt_applicability ({TT_APP_PATH.name} not found)")

    # Print summary
    print(f"\nDatabase: {DB_PATH}")
    tables = con.execute("SHOW TABLES").fetchall()
    for (name,) in tables:
        count = con.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
        print(f"  {name}: {count:,} rows")

    con.close()
    size_mb = DB_PATH.stat().st_size / (1024 * 1024)
    print(f"\nDatabase size: {size_mb:.1f} MB")
    print("Done!")


if __name__ == "__main__":
    main()
