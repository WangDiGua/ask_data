from __future__ import annotations

import argparse
import warnings

from ndea.config import Settings
from ndea.observability import get_health_service
from ndea.runtime import configure_runtime, runtime_support_message
from ndea.server import create_mcp


configure_runtime()
runtime_message = runtime_support_message()
if runtime_message:
    warnings.warn(runtime_message, RuntimeWarning, stacklevel=1)


mcp = create_mcp()
app = mcp


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ndea")
    parser.add_argument("--check", choices=["liveness", "readiness"])
    args = parser.parse_args(argv)

    if args.check is None:
        return 0

    settings = Settings()
    health_service = get_health_service(settings)
    if args.check == "liveness":
        report = health_service.liveness()
        print(report.model_dump_json())
        return 0

    report = health_service.readiness()
    print(report.model_dump_json())
    return 0 if report.readiness else 1


if __name__ == "__main__":
    raise SystemExit(main())
