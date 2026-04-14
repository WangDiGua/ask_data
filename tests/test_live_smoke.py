import os

import pytest

from ndea.config import Settings
from ndea.observability import get_health_service


pytestmark = pytest.mark.skipif(
    os.getenv("NDEA_RUN_LIVE_SMOKE") != "1",
    reason="Live smoke tests require NDEA_RUN_LIVE_SMOKE=1",
)


def test_live_readiness_smoke() -> None:
    report = get_health_service(Settings()).readiness()
    assert report.service == "NDEA"
