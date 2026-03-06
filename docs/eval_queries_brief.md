# Retrieval Eval Queries — Brief

We're building a RAG system over CNH dealer technical documentation (service manuals, diagnostic procedures, fault code guides, service bulletins, etc.). We need a fixed set of test queries to measure how well our search retrieval works.

## What we need

**20-30 realistic technician queries** — the kind of thing a dealer tech would type when stuck on a job. For each query, we'd also like:

1. **The query itself** — natural language, as a tech would phrase it
2. **What a good answer should cover** — key facts, procedures, or specs the system should surface (doesn't need to be exhaustive, just the important bits)
3. **Difficulty hint** (optional) — is this something easy to find with keywords, or would it need deeper understanding?

## What makes a good mix

We want queries that cover different scenarios:

- **Fault code lookups**: "What causes fault code 523774?" / "SCR catalyst efficiency fault"
- **Repair procedures**: "How to replace the DPF filter on a T7.270" / "hydraulic pump removal steps"
- **Spec lookups**: "engine oil capacity for Puma 240" / "torque specs for front axle"
- **Diagnostic reasoning**: "intermittent loss of hydraulic power" / "engine won't start after DPF regen"
- **Parts/tooling**: "special tools needed for clutch replacement"
- **Cross-reference / applicability**: queries where the answer depends on the specific model or variant

## What the docs contain

The corpus is ~930K unique "Information Units" extracted from Arbortext XML, covering:

- Diagnostic procedures (fault code resolution chains, troubleshooting trees)
- Service procedures (remove/install/adjust steps)
- Technical data (specs, torque values, fluid capacities)
- Functional data (wiring diagrams, hydraulic schematics)
- Operating instructions
- Service bulletins
- General information (safety, overview)

Brands: Case, New Holland, STEYR. Equipment: tractors, combines, headers, construction (excavators, loaders, etc.).

## Format

A simple list is fine — something like:

```
Query: What causes fault code 523774 on a T7.270?
Expected: Should reference SCR system diagnostics, DEF quality check, NOx sensor testing procedure. Fault is typically DEF dosing unit or NOx sensor.
Difficulty: Medium — fault code is specific but diagnosis has multiple branches.
```

Or even just a spreadsheet / text file with columns for query, expected content, and notes.

## How we'll use them

Each query gets run through our retrieval system, and an LLM rates each retrieved document chunk as HIGH / MEDIUM / LOW relevance. We track precision over time as we tune the system. The "expected answer" notes help us validate that the LLM judge is rating correctly and catch cases where it's too lenient or strict.
