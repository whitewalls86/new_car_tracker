"""
Plan 112 Gate A2: idempotent Lakekeeper warehouse bootstrap.

Registers the single `cartracker_experiments` warehouse (shared.iceberg_catalog)
against Lakekeeper's management API, storage profile pointed at the isolated
`lakehouse_spike/warehouse/` prefix of the `bronze` MinIO bucket -- never any
other prefix. Iceberg REST /v1/config and namespace CRUD both need a
registered warehouse first (A1's documented limitation); this script is what
makes A2's PySpark round-trip possible.

    python -m scripts.register_lakehouse_warehouse

Idempotent: if a warehouse named `cartracker_experiments` already exists
(checked via GET, and also treated as success on a 409 from POST), this exits
0 without making changes -- safe to call every time before the spike script
runs, in CI, on the VM, or locally.

Exact Lakekeeper management-API request/response shape is pinned to
quay.io/lakekeeper/catalog:v0.13.1 and is the one part of this script most
likely to need adjustment as Lakekeeper's config surface evolves (plan Q6) --
verify against the CI/VM stack, not this script's assumptions alone.
"""
import json
import os
import sys
import urllib.error
import urllib.request

from shared.iceberg_catalog import WAREHOUSE_NAME, warehouse_storage_payload


def _management_base_uri() -> str:
    # LAKEKEEPER_CATALOG_URI is e.g. "http://lakekeeper:8181/catalog"; the
    # management API lives at the same host:port under /management.
    catalog_uri = os.environ["LAKEKEEPER_CATALOG_URI"]
    return catalog_uri.rsplit("/catalog", 1)[0]


def _request(method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    url = f"{_management_base_uri()}{path}"
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Content-Type": "application/json"} if data is not None else {}
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            payload = resp.read()
            return resp.status, (json.loads(payload) if payload else {})
    except urllib.error.HTTPError as e:
        payload = e.read()
        try:
            return e.code, (json.loads(payload) if payload else {})
        except json.JSONDecodeError:
            return e.code, {"raw": payload.decode(errors="replace")}


def warehouse_exists(name: str) -> bool:
    status, body = _request("GET", "/management/v1/warehouse")
    if status != 200:
        raise RuntimeError(f"Failed to list warehouses: HTTP {status} {body}")
    warehouses = body.get("warehouses", [])
    return any(w.get("name") == name for w in warehouses)


def register_warehouse() -> None:
    if warehouse_exists(WAREHOUSE_NAME):
        print(f"Warehouse {WAREHOUSE_NAME!r} already registered; nothing to do.")
        return

    status, body = _request(
        "POST", "/management/v1/warehouse", warehouse_storage_payload()
    )
    if status in (200, 201):
        print(f"Registered warehouse {WAREHOUSE_NAME!r}: {body}")
        return
    if status == 409:
        print(f"Warehouse {WAREHOUSE_NAME!r} already exists (409); treating as success.")
        return
    raise RuntimeError(
        f"Failed to register warehouse {WAREHOUSE_NAME!r}: HTTP {status} {body}"
    )


if __name__ == "__main__":
    try:
        register_warehouse()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
