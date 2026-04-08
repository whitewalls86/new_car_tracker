import os
from typing import Any, Dict, List


def cleanup_artifacts(artifacts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Delete raw artifact files from disk.

    Accepts a list of {"artifact_id": int, "filepath": str} dicts.
    For each, attempts os.remove(). FileNotFoundError is treated as already-deleted (success).
    Returns list of {"artifact_id": int, "deleted": bool, "reason": str|None}.
    """
    results = []
    for item in artifacts:
        artifact_id = item.get("artifact_id")
        filepath = item.get("filepath")

        if not filepath:
            results.append({
                "artifact_id": artifact_id,
                "deleted": False,
                "reason": "no filepath provided",
            })
            continue

        try:
            os.remove(filepath)
            results.append({
                "artifact_id": artifact_id,
                "deleted": True,
                "reason": None,
            })
        except FileNotFoundError:
            # Already gone — treat as success so deleted_at gets set
            results.append({
                "artifact_id": artifact_id,
                "deleted": True,
                "reason": "file not found (already deleted)",
            })
        except Exception as e:
            results.append({
                "artifact_id": artifact_id,
                "deleted": False,
                "reason": f"{type(e).__name__}: {e}",
            })

    return results
