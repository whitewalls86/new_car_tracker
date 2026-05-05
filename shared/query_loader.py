from pathlib import Path


def load_query(sql_dir: Path, name: str) -> str:
    return (sql_dir / f"{name}.sql").read_text()
