#!/usr/bin/env python3
"""Classify scraped job ads with a mock CV matcher."""

from __future__ import annotations

import re
from typing import Any


ENGINEER_WORD = re.compile(r"\bengineer\b", re.IGNORECASE)


def is_mock_hit(job: dict[str, Any]) -> bool:
    description = job.get("description", "")
    if not isinstance(description, str):
        return False
    return bool(ENGINEER_WORD.search(description))
