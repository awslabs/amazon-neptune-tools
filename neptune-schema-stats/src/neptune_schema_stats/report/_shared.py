"""Shared formatting helpers and constants used across the report modules.

Kept intentionally small — anything tied to PG or RDF rendering lives in the
respective sibling module, not here.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from neptune_schema_stats.client.statistics import StatisticsInfo

# ---- Constants for the rendered output ------------------------------------

AMBIGUITY_MARKER = "⚠"  # ⚠ warning triangle
EXACT_MARKER = "✓"  # ✓ checkmark
DASH = "—"  # — em-dash
NUM_TABLEFMT = "simple"


def local_name(uri: str) -> str:
    """Return the local-name portion of an RDF URI (the trailing token after
    ``#`` or ``/``, whichever comes last). Falls back to the full URI if it
    contains neither."""
    for sep in ("#", "/"):
        idx = uri.rfind(sep)
        if idx != -1:
            return uri[idx + 1 :] or uri
    return uri


def pct(numerator: int, denominator: int) -> str:
    """Format ``numerator/denominator`` as a percentage string, or ``"0.0%"``
    when the denominator is zero. Percentages under 0.1% are shown as
    ``"<0.1%"`` rather than rounded to ``"0.0%"`` so they don't visually
    disappear."""
    if denominator == 0:
        return "0.0%"
    ratio = 100.0 * numerator / denominator
    if 0 < ratio < 0.1:
        return "<0.1%"
    return f"{ratio:.1f}%"


def statistics_payload(info: StatisticsInfo) -> dict[str, object]:
    """JSON-serializable projection of :class:`StatisticsInfo` for the
    ``statistics`` field of the report payload. Omits None-valued fields
    so consumers can branch on presence rather than nullness."""
    out: dict[str, object] = {"active": info.active}
    if info.statistics_id is not None:
        out["statistics_id"] = info.statistics_id
    if info.date:
        out["date"] = info.date
    if info.signature_count is not None:
        out["signature_count"] = info.signature_count
    if info.instance_count is not None:
        out["instance_count"] = info.instance_count
    if info.predicate_count is not None:
        out["predicate_count"] = info.predicate_count
    if info.note:
        out["note"] = info.note
    return out
