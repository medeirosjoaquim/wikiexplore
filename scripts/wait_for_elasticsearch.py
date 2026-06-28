"""Wait until Elasticsearch cluster is healthy."""
from __future__ import annotations

import argparse
import os
import sys
import time

import urllib.error
import urllib.request


def main() -> int:
    parser = argparse.ArgumentParser(description="Wait for Elasticsearch")
    parser.add_argument("--url", default=None)
    parser.add_argument("--timeout", default=120, type=int)
    parser.add_argument("--interval", default=3, type=int)
    args = parser.parse_args()

    url = args.url or os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
    health_url = url.rstrip("/") + "/_cluster/health?wait_for_status=yellow&timeout=5s"

    deadline = time.time() + args.timeout
    attempt = 0
    while time.time() < deadline:
        attempt += 1
        try:
            with urllib.request.urlopen(health_url, timeout=10) as resp:
                import json

                body = json.loads(resp.read().decode())
                if body.get("status") in ("green", "yellow"):
                    print(
                        f"[wait_for_elasticsearch] elasticsearch ready "
                        f"(status={body.get('status')}, after {attempt} attempt(s))"
                    )
                    return 0
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            print(f"[wait_for_elasticsearch] attempt {attempt}: {exc}", file=sys.stderr)
        time.sleep(args.interval)
    print("[wait_for_elasticsearch] timed out", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
