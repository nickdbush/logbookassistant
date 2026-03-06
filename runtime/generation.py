"""Query expansion and answer generation (async OpenAI)."""

from __future__ import annotations

DEFAULT_GEN_MODEL = "gpt-5-mini"

MODEL_PRICING = {
    "gpt-5-nano":  {"input": 0.05,  "output": 0.40},
    "gpt-5-mini":  {"input": 0.25,  "output": 2.00},
    "gpt-4o":      {"input": 2.50,  "output": 10.00},
    "text-embedding-3-small": {"input": 0.02, "output": 0.0},
}


def estimate_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    prices = MODEL_PRICING.get(model, {"input": 0, "output": 0})
    return (input_tokens * prices["input"] + output_tokens * prices["output"]) / 1_000_000

SYSTEM_PROMPT = """\
You are a CNH technical assistant helping equipment dealers diagnose and repair \
CNH (Case, New Holland, STEYR) agricultural and construction equipment.

You have access to official service documentation including diagnostic procedures, \
fault codes, repair steps, specification tables, wiring descriptions, and parts info.

Guidelines:
- Provide structured, step-by-step diagnostic and repair guidance
- Always cite source IU IDs when referencing specific procedures (e.g., [Source: 12345678])
- Include relevant fault codes, specifications, and torque values when available
- If the documentation doesn't contain enough info, say so clearly
- Use clear technical language appropriate for dealer technicians
- When multiple procedures apply, list them in logical diagnostic order
"""


async def expand_query(client, query: str) -> list[str]:
    """Generate 3 alternative queries using gpt-5-nano."""
    resp = await client.chat.completions.create(
        model="gpt-5-nano",
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a search query expansion assistant for heavy equipment "
                    "technical documentation (CNH / Case / New Holland tractors, "
                    "combines, construction equipment). Given a technician's query, "
                    "generate exactly 3 alternative search queries, one per line. "
                    "Include synonyms, related technical terms, and fault code variants. "
                    "Output ONLY the 3 queries, no numbering or extra text."
                ),
            },
            {"role": "user", "content": query},
        ],
        max_completion_tokens=200,
    )
    lines = [line.strip() for line in resp.choices[0].message.content.strip().split("\n") if line.strip()]
    expansions = lines[:3]
    model_call = {
        "name": "query_expansion",
        "model": "gpt-5-nano",
        "input_tokens": resp.usage.prompt_tokens,
        "output_tokens": resp.usage.completion_tokens,
        "cost_usd": estimate_cost("gpt-5-nano", resp.usage.prompt_tokens, resp.usage.completion_tokens),
    }
    return expansions, model_call


async def generate_answer(client, query: str, context: str, sources: list[dict], model: str = DEFAULT_GEN_MODEL) -> str:
    """Generate answer from context using the specified model."""
    user_msg = f"""Question: {query}

Reference Documentation:
{context}

Please answer the question using the documentation above. Cite sources using [Source: IU_ID] format."""

    resp = await client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_msg},
        ],
        max_completion_tokens=2000,
    )
    answer = resp.choices[0].message.content
    model_call = {
        "name": "generation",
        "model": model,
        "input_tokens": resp.usage.prompt_tokens,
        "output_tokens": resp.usage.completion_tokens,
        "cost_usd": estimate_cost(model, resp.usage.prompt_tokens, resp.usage.completion_tokens),
    }
    return answer, model_call
