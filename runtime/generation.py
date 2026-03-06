"""Query expansion and answer generation (async OpenAI)."""

from __future__ import annotations

DEFAULT_GEN_MODEL = "gpt-4o"

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
    """Generate 3 alternative queries using gpt-4o-mini."""
    resp = await client.chat.completions.create(
        model="gpt-4o-mini",
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
        temperature=0.7,
        max_tokens=200,
    )
    lines = [line.strip() for line in resp.choices[0].message.content.strip().split("\n") if line.strip()]
    return lines[:3]


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
        temperature=0.2,
        max_completion_tokens=2000,
    )
    return resp.choices[0].message.content
