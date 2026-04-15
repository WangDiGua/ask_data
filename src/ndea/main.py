from __future__ import annotations

import argparse

from ndea.config import Settings
from ndea.http import create_http_app
from ndea.observability import get_health_service
from ndea.server import create_mcp, create_portal_mcp


mcp = create_mcp()
portal_mcp = create_portal_mcp()
app = mcp
http_app = create_http_app()


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
