"""Report rendering — text (concise + detailed) and JSON payloads.

Submodules:

- :mod:`._shared` — constants and small formatters shared across renderers
- :mod:`.pg` — property-graph text rendering
- :mod:`.rdf` — RDF text rendering
- :mod:`.fallback` — fallback-mode text rendering (both PG and RDF)
- :mod:`.payloads` — JSON payload builders
"""

from neptune_schema_stats.report.fallback import (
    render_pg_fallback_report,
    render_rdf_fallback_report,
)
from neptune_schema_stats.report.payloads import (
    pg_fallback_report_payload,
    pg_report_payload,
    rdf_fallback_report_payload,
    rdf_report_payload,
)
from neptune_schema_stats.report.pg import render_pg_report
from neptune_schema_stats.report.rdf import render_rdf_report

__all__ = [
    "pg_fallback_report_payload",
    "pg_report_payload",
    "rdf_fallback_report_payload",
    "rdf_report_payload",
    "render_pg_fallback_report",
    "render_pg_report",
    "render_rdf_fallback_report",
    "render_rdf_report",
]
