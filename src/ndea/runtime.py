from __future__ import annotations

import sys
import warnings


SUPPORTED_MIN = (3, 11)
SUPPORTED_MAX_EXCLUSIVE = (3, 14)


def configure_runtime() -> None:
    if sys.version_info < SUPPORTED_MAX_EXCLUSIVE:
        return
    warnings.filterwarnings(
        "ignore",
        category=UserWarning,
        message=r"Core Pydantic V1 functionality isn't compatible with Python 3\.14 or greater\.",
    )
    warnings.filterwarnings(
        "ignore",
        category=DeprecationWarning,
        message=r"'asyncio\.iscoroutinefunction' is deprecated and slated for removal in Python 3\.16; use inspect\.iscoroutinefunction\(\) instead",
    )


def runtime_support_message() -> str | None:
    version = sys.version_info[:3]
    if SUPPORTED_MIN <= version < SUPPORTED_MAX_EXCLUSIVE:
        return None
    return (
        "NDEA recommends Python 3.11-3.13. "
        f"Current runtime is {version[0]}.{version[1]}.{version[2]}, "
        "which may trigger third-party compatibility warnings."
    )
