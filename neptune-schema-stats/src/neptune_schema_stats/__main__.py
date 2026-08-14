"""Entry point for ``python -m neptune_schema_stats`` and the console script."""

from __future__ import annotations

import sys

from neptune_schema_stats.cli import main

if __name__ == "__main__":
    sys.exit(main())
