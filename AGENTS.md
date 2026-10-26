# JASIL

Framework-agnostic infrastructure substrate for Python services: capability providers (state, storage, events, lock, mail, geocoding), an event pipeline, durable jobs and observability.
Python 3.12+, SQLAlchemy 2, Pydantic 2; Redis, S3, PostgreSQL and SMTP backends sit behind optional extras.
Published to PyPI; the public API is governed by SemVer (`docs/api-stability.md`).

## Where the detail lives

| Working on | Read |
| --- | --- |
| Library code or tests | `.agents/skills/jasil-library/SKILL.md` |
| Fixing a bug, failing test or CI failure | `.agents/skills/debugging/SKILL.md` |
| Commit messages and PRs | `.agents/skills/conventional-commits/SKILL.md` |
| Writing or editing a skill | `.agents/skills/skill-authoring/SKILL.md` |
| What is public and what SemVer covers | `docs/api-stability.md` |
| Providers, backends and URI schemes | `docs/providers-and-backends.md` |
| Deployment profiles and topology | `docs/deployment-profiles.md` |
| Events and the outbox | `docs/events-and-outbox.md` |
| Durable jobs | `docs/durable-jobs.md` |
| Threat model and integration checklist | `docs/security/` |

Skills are plain Markdown; read the relevant `SKILL.md` and the reference files it points to when your tool does not load skills itself.

## Layout

```text
jasil/          Package: providers, events, jobs, event_log, backends/, _core/, migrations/
tests/          Test suite (mirrors the package)
docs/           Documentation site (mkdocs)
.agents/skills/ Agent skills (symlinked into .claude/skills)
```

## Commands

From the repository root (Python 3.12+, `uv`):

```bash
uv sync --all-extras --group dev
uv run ruff check . && uv run ruff format --check .
uv run lint-imports
uv run mypy
uv run pytest
```

## Rules for every change

- Do only what was asked; no extra docs, summaries, READMEs or "helpful" files.
- Prefer existing patterns, utilities and files over new ones.
- KISS: pick the simplest design that solves the underlying problem; no indirection, abstraction layer or option without a current need. Simple means few moving parts, not a small diff or a patch over the symptom.
- YAGNI: build only what the task needs now; no speculative parameters, flags, hooks, config or extension points. What you do build must be complete and proper, not a stub or shortcut.
- DRY: reuse an existing helper, type or fixture before writing another; extract shared code at the third repetition, not the second, and only when the copies change for the same reason.
- Do not change CI workflows, release or publish configuration, supported Python versions, dependency constraints or the `uv` cooldown settings unless asked.
- Never run `git commit` (or any commit-creating command) unless the user explicitly asks; staging for inspection is fine.
- Commit messages and PR titles follow Conventional Commits (enforced in CI); see the `conventional-commits` skill.
- Local-only notes go in `devdocs/`, helper scripts in `devscripts/` (both gitignored).
- No hardcoded secrets; read configuration from the host's settings or environment.
- Ask for clarification when scope is unclear instead of assuming extra deliverables.
- A change touching the public API, more than one top-level package area, or a backend contract starts with a short task list in chat for approval; keep plans in chat or `devdocs/`, never in tracked files.
- Bug fixes and new behavior start with a failing test.
- A change to the public API is done only when `docs/api-stability.md`, the affected docs page and `CHANGELOG.md` agree with the code.
- Done means the checks that apply to what changed ran and their output was read; say which ran and which did not (code checks do not apply to docs, `AGENTS.md` or skill-only changes).
- Before reporting a larger change done, compare the diff to the request and remove anything not asked for.

## Fixing problems

- Fix the root cause, not the symptom. Before changing code, state the cause you found and the evidence for it.
- Do not add workarounds: no special-case branches, swallowed exceptions, retries or sleeps to hide a failure, `type: ignore` / `# noqa`, test skips or loosened assertions, or defensive checks that mask bad state upstream.
- If the proper fix is larger than the request (touches other packages, changes a boundary or the public API, needs a refactor), stop and ask before editing. Present the options:
  1. Proper fix: what it changes and why it is the right one.
  2. Minimal fix: what it leaves broken or the debt it creates.

  Recommend one. Never apply the minimal option without the user's choice.
- If unsure whether a fix is a workaround, treat it as one and ask.

## Building features

- Build the feature the way the architecture intends: correct package, layering and import contracts, even when a shortcut would be quicker.
- Do not take shortcuts: no placeholder or stubbed logic, hardcoded values, TODOs standing in for required behavior, copy-pasted code instead of reuse, logic in the wrong layer, or skipped tests, validation, error handling, migrations or docs.
- Before writing code, state the approach and the files it touches. If more than one reasonable design exists, or the proper design is larger than the request implies, stop and ask. Present the options:
  1. Proper design: what it involves and why it fits the architecture.
  2. Simpler design: what it cuts or defers, and the debt it creates.

  Recommend one. Never apply the simpler option without the user's choice.
- Scope still applies: complete means done properly, not bigger. Do not add features, options or files that were not requested.

## Library invariants

- The import contracts in `pyproject.toml` (`[tool.importlinter]`) are the architecture; a failing contract is a design signal, so fix the import, not the contract.
- Pure ports and the event envelope never import a backend; only the composition root selects one, and an unselected backend's dependency is never loaded.
- `jasil._core` stays a leaf; `jasil` never imports a host application (`core`, `endurain`), enforced by `tests/test_host_agnostic.py`.
- Only what `docs/api-stability.md` lists is public; anything else may change in any release. Renaming or removing a public name is a breaking change.
- The host owns the SQLAlchemy engine and the log configuration; JASIL creates neither and logs with `logging.getLogger(__name__)` and lazy `%`-style arguments.
- Tests are hermetic: no real network, Redis or on-disk database, no sleeping, time injected (`tests/` conventions in the skill).
- Comments and docstrings state what the code does now, not what it used to do.
- Python 3.12 syntax, 120-character lines, Google-style PEP 257 docstrings (`Args`, `Returns`, `Raises` where applicable).
