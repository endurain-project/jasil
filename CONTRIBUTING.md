# Contributing to JASIL

Thank you for considering contributing to JASIL! Before diving in, please read
these guidelines carefully. They exist to make the process sustainable for
everyone.

## A note on maintainership

JASIL is maintained by a single person in their spare time. This means review
bandwidth is genuinely limited. Following these guidelines isn't bureaucracy,
it's what allows contributions to actually get merged rather than sitting in a
queue indefinitely.

## Before you write any code

**Open an issue first.** For anything beyond a small bug fix, typo, or
documentation improvement, please open an issue and wait for a response before
writing code. This takes minutes and can save you hours of work on something that
won't be merged because it conflicts with planned direction, existing work, or
project scope.

If an issue already exists, comment on it to signal your intent so work isn't
duplicated.

## Pull request size — the most important rule

**Keep PRs small and focused on a single concern.**

As a rule of thumb:

- **Target under 300 lines changed** (excluding lock files, generated code, and
  migrations)
- **One PR = one thing.** Don't bundle a bug fix with a refactor with a new
  feature
- If your change is naturally large (e.g. a new backend), break it into a chain
  of smaller PRs that can each be reviewed and merged independently

PRs that are too large to review efficiently will be asked to be split before
they receive a review. This isn't a judgment on the quality of the work, it's a
maintainability constraint.

**Excluded from the line count:** `uv.lock`, migration files, and other generated
or vendored files.

## How to contribute

### Bug fixes

- Check if an issue already exists before opening a new one
- Include clear steps to reproduce in the issue
- Small, targeted fixes are the fastest path to a merge

### Documentation

- Always welcome and rarely requires prior discussion
- Improvements to the docs site, inline code comments, and the README all count
- Keep the same tone and structure as existing docs

### New features

- **Always discuss in an issue first** — this is required, not optional
- Features that haven't been discussed and approved in an issue may be closed
  without review, regardless of quality

### Refactors and code quality

- Must be discussed in an issue first
- Pure refactor PRs (no behaviour change) are easiest to review. Keep them
  separate from feature or fix PRs
- Include a clear explanation of what improved and why

## Getting started

1. **Fork the repository** on GitHub
   ([endurain-project/jasil](https://github.com/endurain-project/jasil)).
   The GitLab copy is a read-only mirror — please don't open PRs there.
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/jasil.git
   cd jasil
   ```
3. **Set up the environment** ([uv](https://docs.astral.sh/uv/) is required; see
   `requires-python` and `[tool.uv] required-version` in `pyproject.toml`):
   ```bash
   uv sync --all-extras --group dev
   ```
4. **Create a branch** with a descriptive name:
   ```bash
   git checkout -b fix/lease-reclaim-off-by-one
   # or
   git checkout -b feat/valkey-state-backend
   ```
5. **Make your changes**, committing with clear messages following
   [Conventional Commits](https://www.conventionalcommits.org/):
   ```
   fix: count the attempt at claim time, not completion
   feat: add a Valkey state backend
   docs: clarify the distributed profile's fail-fast
   ```
   Commit messages are checked in CI.
6. **Push and open a PR** against the `main` branch, filling in the PR template
   completely.

## Before you push

Run the full gate locally — it's the same one CI runs, so this is the fastest way
to avoid a red build:

```bash
uv run ruff check .          # lint
uv run ruff format .         # format
uv run mypy                  # type check
uv run lint-imports          # architectural import contracts
uv run pytest                # tests + coverage gate
```

A few things worth knowing:

- **Tests must be hermetic.** No real network, no real Redis, no on-disk
  database. Redis is faked with `fakeredis`, the database is in-memory SQLite,
  DNS is monkeypatched, and time is injected. Nothing in the suite sleeps.
- **`lint-imports` enforces the architecture.** The pure providers must never
  import a backend, only the composition root selects one, the substrate reaches
  the jobs layer only through the publisher, and `jasil._core` stays a leaf. If
  your change breaks a contract, that's usually a design signal rather than a
  reason to edit the contract — but say so in the PR if you think otherwise.
- **Coverage has a floor** (`fail_under` in `pyproject.toml`). Raise it when
  coverage rises; never lower it to make a build pass.
- **Public API changes need a changelog entry** and must respect
  [API stability](docs/api-stability.md).

## Response time expectations

Reviews may take days to weeks depending on availability. A PR sitting without a
response is not a rejection. Please feel free to leave a polite ping after two
weeks if there's been no activity.

## Thank you
