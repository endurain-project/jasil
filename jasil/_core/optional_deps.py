"""Guard for features behind an optional extra.

Turns a missing optional dependency into one clear message naming the extra to
install, instead of a bare ``ModuleNotFoundError`` from somewhere deep in the
call stack.
"""

from typing import Any

__all__ = ["not_installed", "require"]


def not_installed(package: str, *, extra: str, feature: str) -> ImportError:
    """Build the error that names the extra to install for a missing ``package``.

    Args:
        package: Distribution name, e.g. ``redis``.
        extra: The JASIL extra providing it, e.g. ``redis``.
        feature: Human-readable feature name for the message.

    Returns:
        The ``ImportError`` to raise.
    """
    return ImportError(
        f"{feature} requires the '{package}' package, which is not installed. "
        f"Install it with: pip install 'jasil[{extra}]'"
    )


def require(module: Any, *, package: str, extra: str, feature: str) -> Any:
    """Return ``module``, or raise explaining which extra provides it.

    Args:
        module: The lazily imported module, or ``None`` when the import failed.
        package: Distribution name, e.g. ``alembic``.
        extra: The JASIL extra providing it, e.g. ``migrations``.
        feature: Human-readable feature name for the message.

    Raises:
        ImportError: When ``module`` is ``None``.
    """
    if module is None:
        raise not_installed(package, extra=extra, feature=feature)
    return module
