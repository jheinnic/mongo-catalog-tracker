# Domain Docs

How the engineering skills should consume this repo's domain documentation when exploring the codebase.

## Before exploring, read these

- **`CONTEXT-MAP.md`** at the repo root — it points at one `CONTEXT.md` per context. Read each one relevant to the topic.
- **`docs/adr/`** — system-wide architectural decisions.
- Also check `<context>/docs/adr/` for context-scoped decisions within the relevant context.

If any of these files don't exist, **proceed silently**. Don't flag their absence; don't suggest creating them upfront. The producer skill (`/grill-with-docs`) creates them lazily when terms or decisions actually get resolved.

## Contexts in this repo

This repo has three top-level contexts — not grouped under a common `src/`:

| Directory | Role |
|---|---|
| `mongo_worksim/` | Simulator — models a MongoDB workload and rapidly iterates through it, collecting "daily" index operation counter dumps |
| `orc/` | Observation & analysis — reads the simulator's output, stores analytic results in ORC format |
| `parquet/` | Observation & analysis — identical algorithm to `orc/`, stores analytic results in Parquet format |

### orc/ and parquet/ are siblings — keep them in sync

`orc/` and `parquet/` implement the same algorithm and differ **only** in storage backend. In practice, ~99% of changes to one must be propagated to the other.

**Rules for skills working in either context:**

- After making any logic change to `orc/` or `parquet/`, check whether the same change is needed in the sibling directory.
- When propagating, preserve the sibling's storage format — do not convert ORC files to Parquet or vice versa.
- If a change is intentionally asymmetric (storage-format-specific), say so explicitly rather than silently leaving the sibling behind.
- If asked to work in one context only, still flag at the end: _"This change likely needs to be mirrored in `<sibling>/`."_

## File structure

```
/
├── CONTEXT-MAP.md                     ← root; points to per-context CONTEXT.md
├── docs/adr/                          ← system-wide decisions
├── mongo_worksim/
│   ├── CONTEXT.md
│   └── docs/adr/
├── orc/
│   ├── CONTEXT.md
│   └── docs/adr/
└── parquet/
    ├── CONTEXT.md
    └── docs/adr/
```

## Use the glossary's vocabulary

When your output names a domain concept (in an issue title, a refactor proposal, a hypothesis, a test name), use the term as defined in the relevant `CONTEXT.md`. Don't drift to synonyms the glossary explicitly avoids.

If the concept you need isn't in the glossary yet, that's a signal — either you're inventing language the project doesn't use (reconsider) or there's a real gap (note it for `/grill-with-docs`).

## Flag ADR conflicts

If your output contradicts an existing ADR, surface it explicitly rather than silently overriding:

> _Contradicts ADR-0007 — but worth reopening because…_
