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
