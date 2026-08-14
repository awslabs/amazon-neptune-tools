"""CLI exit codes and JSON schema version.

Consolidated exit-code set. Runtime errors that a script cannot recover
from — connection failed, API error, schema not usable — all return
:data:`EXIT_ERROR`. The two codes that signal *recoverable* conditions
are called out separately: :data:`EXIT_STATS_UNAVAILABLE` (re-run with
``--refresh``) and :data:`EXIT_TIMEOUT` (raise ``--poll-timeout`` or
retry later).
"""

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_USAGE = 2
EXIT_STATS_UNAVAILABLE = 5
EXIT_TIMEOUT = 7

# Stable identifier for the shape of the ``--json`` output. Bump the MAJOR
# component when a field is removed or its meaning changes; bump MINOR
# for additive changes. Consumers should verify this before parsing.
JSON_SCHEMA_VERSION = "1.0"
