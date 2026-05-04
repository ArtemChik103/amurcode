"""Compatibility entrypoint for the local analytics backend."""

from __future__ import annotations

import sys

from analytics import api as _api


if __name__ == "__main__":
    _api.main()
else:
    sys.modules[__name__] = _api
