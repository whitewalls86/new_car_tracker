from pathlib import Path

from shared.query_loader import load_query

_SQL_DIR = Path(__file__).parent / "sql"


def _q(name: str) -> str:
    return load_query(_SQL_DIR, name)


EXPIRE_ORPHAN_DETAIL_CLAIMS = _q("expire_orphan_detail_claims")
