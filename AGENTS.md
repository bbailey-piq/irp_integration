# AGENTS.md

Orientation for coding agents and contributors browsing this repository. The
authoritative guidance travels in the source itself — this file only points at it.

## Where things live

- **Entry point & manager map:** `irp_integration/__init__.py` (module docstring).
- **Cross-cutting HTTP / workflow contracts:** `irp_integration/client.py` (module docstring).
- **Per-area operations:** one manager module each — `edm.py`, `portfolio.py`,
  `mri_import.py`, `treaty.py`, `analysis.py`, `rdm.py`, `reference_data.py`, etc.
- **Generated API reference:** `docs/api.md` — regenerate with
  `python docs/generate_api_docs.py` (never edit by hand).

## Contracts to respect (detailed in `client.py`)

- **Don't double-wrap retries** — the HTTP session already retries 429/5xx with
  exponential backoff across all methods.
- **Check terminal status after polling** — a poll returns on any terminal state
  (`FINISHED`, `FAILED`, or `CANCELLED`); inspect the returned `status` rather
  than assuming success.
- **Names resolve to IDs** — high-level methods accept human-readable names (EDM,
  portfolio, profile, treaty) and look up the IDs internally.

## Conventions & checks

- Docstrings and type hints are the single source of truth; `docs/api.md` is
  generated from them.
- Google-style docstrings, summary on the second line; `py.typed` ships with the
  package.
- Advisory lint (config in `pyproject.toml`): `mypy irp_integration` and
  `ruff check irp_integration`.

## Writing Style

Write clearly and naturally. Applies to chat replies, commit messages, PR
bodies, specs, docs, and code comments.

Name things:

- Use the real name of the thing. Do not replace it with an invented synonym.
- Do not use `genuinely`, `load-bearing`, `leverage`, `robust`, `comprehensive`, `holistic`, `utilize`, `facilitate`, `crucial`, `first-class`, or `it's worth noting`.
- No structural metaphors. Banned: `spine`, `backbone`, `seam`, `surface`, `slice`, `glue`, `plumbing`, `rails`, `guardrails`, `bedrock`, `cornerstone`, `linchpin`, `north star`, `building block`, `primitive`, `first-class citizen`, `footprint`, `surface area`, `ecosystem`, `fabric`, `DNA`. Name the manager, method, endpoint constant, workflow, job, or module instead.
- No inflated verbs. Banned: `unlock`, `empower`, `supercharge`, `streamline`, `elevate`, `drive`, `power`, `harden`, `bake in`, `light up`, `wire up`. Say what the code does.
- If a word stands in for a structure instead of naming it, replace it with the structure's name.
- Avoid vague stand-ins such as `item`, `unit`, `flow`, `piece`, `object`, `entity`, `component`, `layer`, or `handle` when a specific term exists.
- Name the EDM, exposure set, portfolio, account, policy, location, treaty, analysis, workflow, job, manager method, endpoint constant, response field, or environment variable directly.
- Use the API's own terms when writing about API behavior: `exposureId`, `location` header, `FINISHED`, `allowDeepFilters`. Do not paraphrase them.
- Do not write `this`, `that`, `the above`, `the existing behavior`, or `the current approach` when the reference may be unclear.
- Repeat the exact term when needed for clarity. Do not invent a label to avoid repeating a word.
- Do not assume the reader remembers an earlier section or another document.

Say what happened:

- State what happens, who does it, and what changes.
- Lead with the answer. Context comes after, and only if it changes what the reader does next.
- One idea per sentence. Cut any sentence that only restates the one before it.
- Be specific: the number, the file path, the parameter name, the status value, the limit.
- Report the exception, not the inventory. "No violations" beats thirteen rows of "pass".
- Give one recommendation, then the single real risk. Do not hedge both ways.
- Keep descriptions proportional to the change. Length is not evidence of work.
- No preamble, no closing recap.

Bad:

> `client.py` is the backbone of the package and the managers are the glue.

Better:

> `client.py` owns the HTTP session, the retry policy, and workflow polling. Each manager module builds request payloads and calls `Client.request()` or `Client.execute_workflow()`.

Bad:

> `execute_workflow()` returns the object once the operation settles.

Better:

> `execute_workflow()` polls until the workflow reaches `FINISHED`, `FAILED`, or `CANCELLED`, then returns the workflow response. Check `status` — a terminal state is not a success.

Bad:

> Retries are handled at the appropriate layer.

Better:

> The `requests.Session` in `client.py` retries 429, 500, 502, 503, and 504 with exponential backoff. Manager methods do not retry.

Bad:

> The existing flow already handles this.

Better:

> `poll_workflow_batch_to_completion()` polls every workflow ID in one loop; call it instead of looping over `poll_workflow_to_completion()`.

Bad:

> Searches return a limited number of results per call.

Better:

> `search_portfolios()` returns at most `limit` portfolios per call (default 100). `search_portfolios_paginated()` advances `offset` until a short page comes back.

Bad:

> Update this to reflect the decision above.

Better:

> Update the `search_locations()` docstring to state that `state` is filtered through the Search Locations route, not through `allowDeepFilters`.

Bad:

> The manager surfaces a friendly error when the lookup fails.

Better:

> `get_analysis_by_name()` raises `IRPValidationError` when `analysis_name` or `edm_name` is empty, and `IRPAPIError` when the search returns zero or more than one analysis. Both messages name the analysis and the EDM.

Length:

- Commit subject ≤ 72 characters. The body says why; the diff says what.
- PR descriptions scale with the diff: what changed and why, then how to verify.
- Chat replies answer the question asked. No status inventories, no tables of completed work.
