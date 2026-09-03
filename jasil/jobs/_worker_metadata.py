"""Validation for host-supplied durable-worker descriptors."""

import json
from typing import Any

from jasil._core.limits import MAX_WORKER_LABEL_LENGTH, MAX_WORKER_ROLE_LENGTH, check_length

MAX_WORKER_METADATA_BYTES = 16 * 1024


def normalize_worker_metadata(
    *,
    role: str | None,
    label: str | None,
    metadata: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Validate worker descriptors and return a detached metadata mapping."""
    if role is not None:
        check_length(role, field="role", limit=MAX_WORKER_ROLE_LENGTH)
    if label is not None:
        check_length(label, field="label", limit=MAX_WORKER_LABEL_LENGTH)
    normalized = dict(metadata) if metadata is not None else None
    if normalized is None:
        return None
    try:
        encoded = json.dumps(
            normalized,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode()
    except (TypeError, ValueError) as error:
        raise ValueError("worker metadata must be JSON serializable") from error
    if len(encoded) > MAX_WORKER_METADATA_BYTES:
        raise ValueError(
            f"worker metadata is {len(encoded)} bytes, which exceeds the {MAX_WORKER_METADATA_BYTES}-byte limit"
        )
    return normalized
