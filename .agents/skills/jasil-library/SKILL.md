---
name: jasil-library
description: JASIL library conventions (providers, backends, events, jobs, public API). Use when adding, moving, refactoring or reviewing code in jasil/ or tests/.
---

# JASIL library

`docs/api-stability.md` defines the public surface and `[tool.importlinter]` in `pyproject.toml` defines the architecture.
Where this skill and either disagree, they win.

## Core rules (always apply)

- A pure port (`providers`, `events`, `profile`, `capabilities`, `settings`, ...) never imports a backend, `container`, `jobs` or `event_log`; only the composition root selects a backend.
- A backend's third-party dependency is imported lazily, so an unselected extra is never loaded and `import jasil` stays dependency-light.
- If a contract blocks you, the design is wrong; do not edit the contract or add `ignore_imports` to pass.
- `jasil._core` is a leaf and private; the substrate reaches the jobs layer only through `jasil.publisher`.
- Never import a host application (`core`, `endurain`); JASIL takes everything from settings, providers and the host's sessionmaker.
- A new public name goes in `jasil/__init__.py` or a listed module, in the `docs/api-stability.md` table, in the docs page that explains it and in `CHANGELOG.md`.
- A rename, removal, signature change, event-envelope change, URI-scheme change or change to JASIL's own tables is breaking: mark it `!` and say so in the changelog.
- Schema changes ship as an explicit Alembic revision in `jasil/migrations/versions/`; an old-release writer must stay valid during a rollout.
- A documented `import jasil...` snippet must resolve; `python .github/scripts/check_docs_api.py` checks it.
- KISS, YAGNI, DRY: search the package for an existing helper first; add no parameter, wrapper, protocol or setting without a current caller; extract on the third repetition.
- Comments and docstrings say what the code does now and why; history goes in the commit message.

## Read on demand

| When you are | Read |
| --- | --- |
| Writing code, docstrings, logging, errors or typing | `references/code-standards.md` |
| Writing or fixing tests | `references/testing.md` |
| Touching secrets, outbound calls, SQL, locks or anything on the threat model | `references/security.md` |

## Before finishing

```bash
uv run ruff check . && uv run ruff format --check .
uv run lint-imports
uv run mypy
uv run pytest
python .github/scripts/check_module_coverage.py
python .github/scripts/check_docs_api.py
```

`uv run pytest` enforces the `fail_under` total; `check_module_coverage.py` then reads that run and enforces an 80% floor per module.
`uv run ty check` is advisory in CI and not a gate.

## Repo hygiene

- Do only what was asked: no extra docs, summaries or READMEs.
- Prefer existing patterns and utilities over new files; keep it simple, build only what is needed now, do not repeat existing code.
- Do not touch CI, publish configuration, supported Python versions or dependency constraints unless asked.
- Local notes go in `devdocs/`, helper scripts in `devscripts/` (gitignored).
- Never run `git commit` unless asked; use the `conventional-commits` skill for messages.
