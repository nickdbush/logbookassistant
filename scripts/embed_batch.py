"""Embed chunks using OpenAI real-time /v1/embeddings endpoint.

Sends batches of up to 2048 chunks per request with async concurrency.
Rate-limits to stay under TPM limit. Checkpoints progress to resume
after interruption.

Outputs:
- data/corpus/embeddings.npy   (float32 memmap, shape N×1536)
- data/corpus/embedding_index.parquet  (chunk_id → row index)
- data/corpus/chunk_ids.npy
"""

import argparse
import asyncio
import json
import math
import time
from collections import deque
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import tiktoken
from dotenv import load_dotenv
from openai import AsyncOpenAI, RateLimitError, APIStatusError

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

CHUNKS_PATH = ROOT / "data" / "corpus" / "chunks.parquet"
EMBEDDINGS_PATH = ROOT / "data" / "corpus" / "embeddings.npy"
INDEX_PATH = ROOT / "data" / "corpus" / "embedding_index.parquet"
CHUNK_IDS_PATH = ROOT / "data" / "corpus" / "chunk_ids.npy"
CHECKPOINT_PATH = ROOT / "data" / "corpus" / "embed_checkpoint.json"

MODEL = "text-embedding-3-small"
EMBED_DIM = 1536
MAX_INPUTS_PER_REQUEST = 2048
MAX_TOKENS_PER_REQUEST = 250_000  # API limit is 300k; OpenAI adds per-input overhead
PRICE_PER_MILLION_TOKENS = 0.020  # real-time pricing

# Rate limiting
TPM_LIMIT = 1_000_000
MAX_CONCURRENT = 8
CHECKPOINT_EVERY = 1000  # requests

# Retry
MAX_RETRIES = 8
BASE_DELAY = 1.0


class TokenBucket:
    """Sliding-window TPM tracker."""

    def __init__(self, limit: int, window: float = 60.0):
        self.limit = limit
        self.window = window
        self.entries: deque[tuple[float, int]] = deque()
        self.total = 0

    def _expire(self):
        cutoff = time.monotonic() - self.window
        while self.entries and self.entries[0][0] < cutoff:
            _, tokens = self.entries.popleft()
            self.total -= tokens

    def available(self) -> int:
        self._expire()
        return self.limit - self.total

    def consume(self, tokens: int):
        now = time.monotonic()
        self.entries.append((now, tokens))
        self.total += tokens

    async def wait_for(self, tokens: int):
        while True:
            self._expire()
            if self.total + tokens <= self.limit:
                self.consume(tokens)
                return
            # Wait until oldest entry expires
            wait = self.entries[0][0] + self.window - time.monotonic() + 0.1
            await asyncio.sleep(max(0.1, wait))


class Progress:
    """Track embedding progress."""

    def __init__(self, total_chunks: int, total_tokens: int,
                 completed_requests: int, completed_chunks: int,
                 completed_tokens: int):
        self.total_chunks = total_chunks
        self.total_tokens = total_tokens
        self.completed_requests = completed_requests
        self.completed_chunks = completed_chunks
        self.completed_tokens = completed_tokens
        self.start_time = time.monotonic()
        self.lock = asyncio.Lock()

    async def update(self, chunks: int, tokens: int):
        async with self.lock:
            self.completed_requests += 1
            self.completed_chunks += chunks
            self.completed_tokens += tokens

    def log(self):
        elapsed = time.monotonic() - self.start_time
        pct = self.completed_chunks / self.total_chunks * 100
        cost_so_far = self.completed_tokens / 1_000_000 * PRICE_PER_MILLION_TOKENS
        total_cost = self.total_tokens / 1_000_000 * PRICE_PER_MILLION_TOKENS

        if self.completed_chunks > 0 and elapsed > 0:
            rate = self.completed_chunks / elapsed
            remaining = (self.total_chunks - self.completed_chunks) / rate
            eta = f"{remaining / 60:.0f}m" if remaining > 60 else f"{remaining:.0f}s"
            tpm = self.completed_tokens / elapsed * 60
        else:
            eta = "?"
            tpm = 0

        print(f"  [{pct:5.1f}%] {self.completed_chunks:,}/{self.total_chunks:,} chunks | "
              f"req {self.completed_requests:,} | "
              f"${cost_so_far:.2f}/${total_cost:.2f} | "
              f"{tpm:,.0f} TPM | "
              f"ETA {eta}")


async def embed_request(client: AsyncOpenAI, texts: list[str],
                        bucket: TokenBucket, token_count: int,
                        semaphore: asyncio.Semaphore) -> list[list[float]]:
    """Send one embedding request with rate limiting and retries."""
    await bucket.wait_for(token_count)

    async with semaphore:
        for attempt in range(MAX_RETRIES):
            try:
                resp = await client.embeddings.create(
                    model=MODEL, input=texts
                )
                # Sort by index to guarantee order
                data = sorted(resp.data, key=lambda x: x.index)
                return [d.embedding for d in data]
            except RateLimitError as e:
                delay = BASE_DELAY * (2 ** attempt)
                headers = e.response.headers if e.response else {}
                remaining_req = headers.get("x-ratelimit-remaining-requests", "?")
                remaining_tok = headers.get("x-ratelimit-remaining-tokens", "?")
                reset_req = headers.get("x-ratelimit-reset-requests", "?")
                reset_tok = headers.get("x-ratelimit-reset-tokens", "?")
                print(f"    429 rate limit: {e.message}")
                print(f"      remaining: {remaining_req} req, {remaining_tok} tokens | "
                      f"resets: req {reset_req}, tokens {reset_tok}")
                print(f"      retry {attempt + 1}/{MAX_RETRIES} in {delay:.0f}s "
                      f"(request had {len(texts)} inputs, {token_count:,} tokens)")
                await asyncio.sleep(delay)
            except APIStatusError as e:
                if e.status_code >= 500:
                    delay = BASE_DELAY * (2 ** attempt)
                    print(f"    {e.status_code} server error, retry {attempt + 1} in {delay:.0f}s")
                    await asyncio.sleep(delay)
                else:
                    raise

        raise RuntimeError(f"Failed after {MAX_RETRIES} retries")


def build_requests(token_counts, total):
    """Pack chunks into requests respecting both input and token limits."""
    requests = []
    i = 0
    while i < total:
        req_tokens = 0
        req_count = 0
        start = i
        while i < total and req_count < MAX_INPUTS_PER_REQUEST:
            t = token_counts[i]
            if req_tokens + t > MAX_TOKENS_PER_REQUEST and req_count > 0:
                break
            req_tokens += t
            req_count += 1
            i += 1
        requests.append((len(requests), start, i, req_tokens))
    return requests


async def run(args):
    # Load data
    print("Loading chunks.parquet...")
    table = pq.read_table(CHUNKS_PATH, columns=["chunk_id", "text"])
    chunk_ids = table.column("chunk_id").to_pylist()
    texts = table.column("text").to_pylist()
    total = len(chunk_ids)

    # Count tokens with tiktoken (cl100k_base for text-embedding-3-small)
    print("Counting tokens with tiktoken...")
    enc = tiktoken.get_encoding("cl100k_base")
    token_counts = enc.encode_batch(texts, num_threads=8)
    token_counts = [len(t) for t in token_counts]
    total_tokens = sum(token_counts)
    print(f"  Tokenized {total:,} chunks → {total_tokens:,} tokens")

    # Build variable-size requests
    requests = build_requests(token_counts, total)
    num_requests = len(requests)

    print(f"  {total:,} chunks, {total_tokens:,} tokens")
    print(f"  {num_requests:,} requests (packed to ≤{MAX_TOKENS_PER_REQUEST:,} tokens "
          f"and ≤{MAX_INPUTS_PER_REQUEST:,} inputs each)")

    cost = total_tokens / 1_000_000 * PRICE_PER_MILLION_TOKENS
    print(f"  Model: {MODEL}")
    print(f"  Estimated cost: ${cost:.2f} "
          f"({total_tokens / 1_000_000:.1f}M tokens × ${PRICE_PER_MILLION_TOKENS}/1M)")

    if args.dry_run:
        sizes = [end - start for _, start, end, _ in requests]
        print(f"\n  Requests: {num_requests:,}")
        print(f"  Chunks/request: min={min(sizes)}, max={max(sizes)}, "
              f"avg={sum(sizes)/len(sizes):.0f}")
        print(f"  Concurrency: {MAX_CONCURRENT}")
        print(f"  TPM limit: {args.tpm_limit:,}")
        print(f"  Embeddings output: ({total:,}, {EMBED_DIM}) float32 "
              f"≈ {total * EMBED_DIM * 4 / (1024**3):.1f} GB")
        print("\n  Dry run complete — nothing sent.")
        return

    # Save ordered chunk_ids
    CHUNK_IDS_PATH.parent.mkdir(parents=True, exist_ok=True)
    np.save(CHUNK_IDS_PATH, np.array(chunk_ids, dtype=object))

    # Load or create checkpoint (tracks contiguous chunk boundary)
    start_request = 0
    done_chunks = 0
    done_tokens = 0
    if CHECKPOINT_PATH.exists():
        checkpoint = json.loads(CHECKPOINT_PATH.read_text())
        done_chunks = checkpoint["completed_chunks"]
        done_tokens = checkpoint["completed_tokens"]
        # Find the first request at or past the checkpoint boundary
        for idx, (_, s, e, _) in enumerate(requests):
            if s >= done_chunks:
                start_request = idx
                break
        else:
            start_request = num_requests
        print(f"  Resuming from request {start_request:,} "
              f"(chunk {done_chunks:,}/{total:,})")

    # Create/open memmap
    if start_request == 0:
        embeddings = np.memmap(EMBEDDINGS_PATH, dtype=np.float32, mode="w+",
                               shape=(total, EMBED_DIM))
    else:
        embeddings = np.memmap(EMBEDDINGS_PATH, dtype=np.float32, mode="r+",
                               shape=(total, EMBED_DIM))

    progress = Progress(total, total_tokens, start_request, done_chunks, done_tokens)
    bucket = TokenBucket(args.tpm_limit)
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    client = AsyncOpenAI()

    log_interval = max(1, num_requests // 200)  # ~200 log lines

    async def process_request(req_idx, start, end, req_tokens):
        req_texts = texts[start:end]
        embs = await embed_request(client, req_texts, bucket, req_tokens, semaphore)

        # Write to memmap
        embeddings[start:end] = embs

        await progress.update(end - start, req_tokens)

        if progress.completed_requests % log_interval == 0:
            progress.log()

    # Run remaining requests with limited concurrency
    pending_requests = requests[start_request:]
    print(f"\n  Sending {len(pending_requests):,} requests...")

    # Process in waves — checkpoint after each wave completes so
    # resume always starts from a contiguous boundary
    WAVE_SIZE = MAX_CONCURRENT * 4
    for wave_start in range(0, len(pending_requests), WAVE_SIZE):
        wave = pending_requests[wave_start:wave_start + WAVE_SIZE]
        tasks = [process_request(*r) for r in wave]
        await asyncio.gather(*tasks)

        # All requests in wave succeeded — checkpoint at the last
        # chunk index in this wave, which is contiguous with prior waves
        last_req_end = wave[-1][2]  # end index of last request in wave
        wave_tokens = sum(r[3] for r in wave)
        embeddings.flush()
        CHECKPOINT_PATH.write_text(json.dumps({
            "completed_chunks": last_req_end,
            "completed_tokens": progress.completed_tokens,
        }))

    # Final flush and checkpoint
    embeddings.flush()
    progress.log()

    # Write final index
    print("\nWriting embedding_index.parquet...")
    index_table = pa.table({
        "chunk_id": pa.array(chunk_ids, type=pa.string()),
        "embedding_row": pa.array(range(total), type=pa.int32()),
    })
    pq.write_table(index_table, INDEX_PATH)

    # Clean up checkpoint
    CHECKPOINT_PATH.unlink(missing_ok=True)

    # Verify
    zero_rows = int(np.sum(np.all(embeddings == 0, axis=1)))
    if zero_rows > 0:
        print(f"WARNING: {zero_rows:,} rows are all zeros")
    else:
        print("All rows have non-zero embeddings")

    del embeddings
    size_gb = EMBEDDINGS_PATH.stat().st_size / (1024**3)
    print(f"\n  embeddings.npy: {size_gb:.1f} GB")
    print(f"  embedding_index.parquet: {len(index_table):,} rows")
    print("Done!")


def main():
    parser = argparse.ArgumentParser(description="Embed chunks via OpenAI real-time API")
    parser.add_argument("--dry-run", action="store_true",
                        help="Calculate cost and show plan without sending")
    parser.add_argument("--tpm-limit", type=int, default=TPM_LIMIT,
                        help=f"Tokens-per-minute limit (default: {TPM_LIMIT:,})")
    args = parser.parse_args()

    asyncio.run(run(args))


if __name__ == "__main__":
    main()
