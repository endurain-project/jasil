# Code standards

## Python
- Python 3.12 syntax (`requires-python = ">=3.12"`): `int | None`, `list[str]`; no `typing.Optional/List/Dict`. Do not use syntax newer than 3.12.
- Lines up to 120 characters, code and prose alike (ruff format does not wrap docstrings; keep paragraphs readable).
- Imports grouped stdlib / third-party / local; `TYPE_CHECKING` to avoid cycles.
- Every module has a top-level docstring.
- Comments and docstrings state what the code does now and why. History goes in the commit message.

## Docstrings (PEP 257, Google sections)
- Summary on the opening line, then a blank line, then sections.
- Every public module, class, function and method has a docstring.
- `Args`: every parameter of a public function. A `_private` helper gets a summary and `Args` only for a non-obvious parameter.
- `Returns`: when a value comes back. `Returns: None.` is not required.
- `Raises`: the exceptions the function raises or deliberately lets propagate. Omit it when there are none.
- Classes: summary plus `Attributes`.
- No examples in docstrings; they belong in `docs/`, and `check_docs_api.py` verifies those.
- A docstring is not a design document: what, then why, in the fewest lines.
- A docstring with backslashes is a raw string (`r"""`).

## Typing
- `mypy` is the gate (`check_untyped_defs`); every new function is fully annotated.
- Do not add `# type: ignore` or `# noqa` to pass; fix the type or the code. An existing targeted ignore stays only where `pyproject.toml` or a comment explains it.
- ORM model modules bind to the host-owned base at runtime and are excluded from mypy; do not give them host-specific types.

## Logging
- `logging.getLogger(__name__)`; the host owns handlers, levels and formatting, so never call `basicConfig`, add handlers or set levels.
- Lazy `%`-style arguments (`log.warning("lost lease %s", job_id)`), never an f-string (ruff `G`/`LOG` enforce it).
- Never log secrets, credentials, tokens, message bodies or full URLs with credentials.
- Error-message wording and log text are not public API.

## Errors
- Raise a specific exception with a message that is safe to show a caller; keep diagnostics (SQL, paths, stack) in the log with `exc_info`.
- Do not swallow an exception to keep going; a failure a caller must decide on propagates.
- A retryable failure (mail, provider) says so in its type, as `MailError` does.

## Dependencies and extras
- The core depends on `sqlalchemy` and `pydantic` only; anything else belongs behind an extra in `pyproject.toml`.
- Adding or changing a dependency is a request-level decision: ask first.

## SQLAlchemy 2
- `Mapped[...]` with `mapped_column()`; every column typed.
- JASIL never creates the engine or commits the host's transaction beyond what the documented API promises; follow the existing `after_commit` and unit-of-work patterns.
