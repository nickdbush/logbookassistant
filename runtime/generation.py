"""Query expansion (OpenAI) and answer generation (Anthropic)."""

from __future__ import annotations

import json as _json
from typing import Literal

from pydantic import BaseModel

DEFAULT_GEN_MODEL = "claude-sonnet-4-6"

MODEL_PRICING = {
    "gpt-5-nano":  {"input": 0.05,  "output": 0.40},
    "claude-sonnet-4-6": {"input": 3.00, "output": 15.00},
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

## Clarifying Questions
When the technician's question is ambiguous or you need more information for an \
accurate diagnosis, ask clarifying questions. Always provide what help you can in \
your answer first, then ask questions to refine.

Question types:
- single_select: pick one option (use for yes/no with "Yes"/"No" options)
- multi_select: select all that apply (symptoms, conditions)
- number: request a measurement (set unit, min, max, step)

Rules:
- Ask 1-3 questions max per response
- Make questions specific and actionable
- Always provide options that cover the likely cases
- The technician can always type a freeform response instead
- Set questions to null when you have enough information
"""

class SelectOption(BaseModel):
    label: str
    value: str


class ResponseQuestion(BaseModel):
    id: str
    type: Literal["single_select", "multi_select", "number"]
    text: str
    options: list[SelectOption] | None = None
    min: float | None = None
    max: float | None = None
    step: float | None = None
    unit: str | None = None


class AssistantResponse(BaseModel):
    answer: str
    questions: list[ResponseQuestion] | None = None


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


def _format_user_turn(turn: dict, question_map: dict[str, str] | None = None) -> str:
    """Format a user conversation turn into readable text for the LLM.

    question_map: mapping of question ID → question text from the preceding
    assistant turn, used to label answers with the question rather than an
    opaque ID.
    """
    qmap = question_map or {}
    parts = []
    if turn.get("answers"):
        for ans in turn["answers"]:
            qid = ans.get("question_id", "?")
            label = qmap.get(qid, qid)
            if ans.get("selected"):
                parts.append(f"{label}: {', '.join(ans['selected'])}")
            elif ans.get("number") is not None:
                parts.append(f"{label}: {ans['number']}")
    if turn.get("text"):
        parts.append(turn["text"])
    return "\n".join(parts) if parts else "(no response)"


async def generate_answer(
    client,
    query: str,
    context: str,
    sources: list[dict],
    model: str = DEFAULT_GEN_MODEL,
    conversation: list[dict] | None = None,
) -> tuple[str, list[dict] | None, dict]:
    """Generate answer from context using Anthropic structured output."""
    user_msg = f"""Question: {query}

Reference Documentation:
{context}

Please answer the question using the documentation above. Cite sources using [Source: IU_ID] format."""

    messages = [{"role": "user", "content": user_msg}]

    if conversation:
        last_questions: dict[str, str] = {}
        for turn in conversation:
            if turn.get("role") == "assistant":
                last_questions = {}
                if turn.get("questions"):
                    for q in turn["questions"]:
                        last_questions[q["id"]] = q["text"]
                messages.append({
                    "role": "assistant",
                    "content": _json.dumps({
                        "answer": turn.get("text", ""),
                        "questions": turn.get("questions"),
                    }),
                })
            elif turn.get("role") == "user":
                messages.append({
                    "role": "user",
                    "content": _format_user_turn(turn, last_questions),
                })
                last_questions = {}

    resp = await client.messages.parse(
        model=model,
        system=SYSTEM_PROMPT,
        messages=messages,
        max_tokens=2000,
        output_format=AssistantResponse,
    )
    parsed = resp.parsed_output
    answer = parsed.answer
    questions = [q.model_dump() for q in parsed.questions] if parsed.questions else None

    model_call = {
        "name": "generation",
        "model": model,
        "input_tokens": resp.usage.input_tokens,
        "output_tokens": resp.usage.output_tokens,
        "cost_usd": estimate_cost(model, resp.usage.input_tokens, resp.usage.output_tokens),
    }
    return answer, questions, model_call
